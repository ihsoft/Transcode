#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Version: 1.0

import argparse
import datetime
import glob
import json
import re
import subprocess
import sys
import os
import logging
import logging.handlers
import textwrap
import tempfile
import time


# All folder and file names will be encoded in this encoding before being passed to the OS for
# execution. To know the encoding starts the console and execute:
# - On Linux: echo $LANG
# - On Windows: chcp
CONSOLE_ENCODING = 'utf-8'


# Lookup for log level settings.
LOG_LEVELS = [
  logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG
]


class Error(Exception):
  pass


class VideoDataError(Error):
  pass


# Simple ENUM-like class to wrap choice settings.
class ChoiceSetting(object):

  def __init__(self, setting_name, **enums):
    self.__name = setting_name
    self.__dict = dict(enums)
    self.__dict__.update(**enums)

  def Check(self, value):
    if value not in self.__dict.values():
      raise Error(
          'SETTINGS: Unknown value for %s: %s. Allowed: %s'
          % (self.__name, value, ','.join(self.__dict.values())))


# Wrapper to convert dict into object (vars "reverse").
class Struct(object):
  def __init__(self, **entries):
    self.__transform_dict(entries)

  def __transform_dict(self, src_dict):
    for key, value in src_dict.iteritems():
      if isinstance(value, dict):
        self.__dict__[key] = Struct(**value)
      else:
        self.__dict__[key] = value

  def __str__(self):
    return str(self.__dict__);

  def __repr__(self):
    return str(self.__dict__);

# Wrapper for a stream. Esnures that base 'tags' always exist.
class Stream(Struct):

  def __init__(self, settings, **json):
    super(Stream, self).__init__(**json)
    if not hasattr(self, 'tags'):
      self.__dict__['tags'] = Struct()
    if not hasattr(self.tags, 'title'):
      self.tags.__dict__['title'] = 'N/A'
    if not hasattr(self.tags, 'language'):
      self.tags.__dict__['language'] = 'N/A'

  def __str__(self):
    return str(self.__dict__);


# Strict type for folder mode.
OwnFolderMode = ChoiceSetting('OwnFolderMode', Never='never', Subs='subs', Always='always')


# Strict type for cleanup mode.
CleanupMode = ChoiceSetting('CleanupMode', Keep='keep', Drop='drop', Move='move')


# Global session stats.
STATS = Struct(
    # Total # of files matched the patterns.
    scanned=0,
    # Total # of macthed files skipped due to different reasons (e.g. already done).
    skipped=0,
    # Total # of files remuxed into MP4 (h264/AAC).
    files_remuxed=0,
    # Number of files that required video transcoding.
    video_transcoded=0,
    # Number of files that required audio transcoding.
    audio_transcoded=0,
    # Number of failed remuxing.
    errors=0
)
    

# Main class for remuxing single file. Normally the following method need to be called:
# - VideoData.ResolveFile() - populates all paths.
# - VideoData.Transcode() - does actual remux/transcode.
# - VideoData.Cleanup() - deals with the source file.
class VideoData(object):

  logger = None

  # Metadata returned by ffprobe fro the file.
  metadata = None

  # Video stream that matched 'vcodec_*' settings.
  video_stream = None

  # Steram with image for the cover.
  cover_stream = None

  # Audio stream that matched 'acodec_*' settings.
  audio_stream = None

  # Subtitles stream that matched 'scodec_*' settings.
  subtitles_stream = None

  # Full path to the source.
  source_path = None

  # Directory location of the source.
  source_location = None

  # Source filename without the extension.
  source_filename = None

  # Directory location of the target.
  target_location = None

  # Destination filename without the extension.
  target_filename = None


  def __init__(self, settings, video_path, logger):
    self.settings = Struct(**settings)
    self.logger = logger
    self.source_path = os.path.abspath(video_path)
    self.source_location = os.path.dirname(self.source_path)
    self.source_filename, _ = os.path.splitext(os.path.basename(self.source_path))

    if not self.settings.target_folder:
      # Remux in the source's folder
      self.target_location = self.source_location
    else:
      self.target_location = os.path.abspath(self.settings.target_folder)
    self.target_filename = self.source_filename

  def ResolveFile(self):
    if not os.path.exists(self.source_path):
      raise VideoDataError('Cannot find source file')
    self.logger.info('Resolve file: %s', self.source_path)
    self.ExtractInfo()

    # Wrap target into own folder if needed.
    OwnFolderMode.Check(self.settings.make_own_folder)
    if (self.settings.make_own_folder == OwnFolderMode.Subs
        and (self.subtitles_stream or self.cover_stream)
        or self.settings.make_own_folder == OwnFolderMode.Always):

      if os.path.basename(self.source_location) != self.source_filename:
        self.target_location = os.path.join(self.target_location, self.source_filename)
      else:
        self.logger.debug('Not using own folder since already in it');

    # Check if there are external subs for the file. We won't pick them up!
    for fn in os.listdir(self.source_location):
      if (fn.startswith(self.source_filename)
          and os.path.basename(self.source_path) != fn
          and not fn.endswith('.status')):
        self.logger.warning('Ignoring external subtitles file: %s', fn)

  def Transcode(self, overwrite_existing=False):
    global STATS

    target_path = os.path.join(self.target_location, self.target_filename + '.mp4')
    target_status = target_path + '.status'
    if os.path.isfile(target_status):
      self.logger.warning('Previous remuxing attempt has failed. Overwriting target.')
    elif not overwrite_existing and os.path.isfile(target_path):
      self.logger.info('Skip transcoding since target already exists: %s', target_path)
      STATS.skipped += 1
      return

    if not self.video_stream or not self.audio_stream:
      raise VideoDataError(
          'Required streams missed: video=%s, audio=%s' % (
          self.video_stream != None, self.audio_stream != None))

    self.logger.info('Remux source file: %s', self.source_path)

    # Input file and A/V streams.
    args = []
    if self.settings.ffmpeg_global_opts:
      args.extend(self.settings.ffmpeg_global_opts)
    args.extend(['-v', 'quiet', '-i', self.source_path])
    args.extend(['-map', '0:%s' % self.video_stream.index])
    args.extend(['-map', '0:%s' % self.audio_stream.index])

    # Video stream copy/transcoding.
    if self.video_stream.codec_name == self.settings.vcodec_type:
      args.extend(['-c:v', 'copy'])
    else:
      if not self.settings.vcodec_transcode:
        raise VideoDataError('Video transcoding not allowed')
      self.logger.warning(
          'Transcode VIDEO stream from %s to h264 using default settings',
          self.video_stream.codec_name)
      args.append('-c:v')
      args.extend(self.settings.vcodec_transcode)
      if not self.settings.dry_run:
        STATS.video_transcoded += 1

    # Audio stream copy/transcoding.
    if (self.audio_stream.codec_name == self.settings.acodec_type
        and self.audio_stream.channels == self.settings.acodec_channels):
      args.extend(['-c:a', 'copy'])
    else:
      self.logger.info(
          'Transcode AUDIO stream from "%s:%d" to "%s:%d"',
          self.audio_stream.codec_name, self.audio_stream.channels,
          self.settings.acodec_type, self.settings.acodec_channels)
      args.append('-c:a')
      args.extend(self.settings.acodec_transcode)
      if not self.settings.dry_run:
        STATS.audio_transcoded += 1

    # Output MP4. Always overwrite, or it may hang!
    self.logger.info('Remux target location: %s', self.target_location)
    args.extend(['-y', target_path])

    # Output SRT if there is forced subtitles track.
    sub_path = None
    if self.subtitles_stream:
      sub_path = os.path.join(
          self.target_location,
          '%s.%s.forced.srt' % (self.source_filename, self.settings.scodec_lang))
      self.logger.info('Store external subtitles in: %s', os.path.basename(sub_path))
      args.extend([
          '-map', '0:%s' % self.subtitles_stream.index, '-c:s', 'srt', '-y', sub_path])

    # Output cover if there is a cover image found.
    cover_path = None
    if self.cover_stream:
      cover_path = os.path.join(self.target_location, self.cover_stream.tags.filename)
      self.logger.info('Store cover image in: %s', os.path.basename(cover_path))
      args.extend([
          '-map', '0:%s' % self.cover_stream.index, '-c:v', 'copy', '-y', cover_path])

    # Ensure output paths are good for ffmpeg to output.
    self.MakeFolderExisting(self.target_location)
    self.EnsureFilePathIsUnused(target_path)
    if sub_path:
      self.EnsureFilePathIsUnused(sub_path)

    # Run FFMPEG to do the work.
    start_time = time.time()
    self.logger.info('Start remuxing...')

    if not self.settings.dry_run:
      with open(target_status, 'w') as fp:
        fp.write(str(start_time))
    else:
      self.logger.info('DRY RUN: Not writing status file: %s', target_status)
    self.ExecuteFFMPEG(*args)
    if not self.settings.dry_run:
      os.unlink(target_status)
    else:
      self.logger.info('DRY RUN: Not dropping status file: %s', target_status)
    self.logger.info('Remux DONE. Time spent: %s',
                     datetime.timedelta(seconds=time.time() - start_time))

    # Update stats.
    if self.settings.dry_run:
      STATS.skipped += 1
    else:
      STATS.files_remuxed += 1

  def Cleanup(self):
    self.logger.info('Cleanup file: %s', self.source_path)
    CleanupMode.Check(self.settings.cleanup_mode)
    if self.settings.cleanup_mode == CleanupMode.Keep:
      self.logger.info('Not cleaning up source file due to settings')
      return
    if self.settings.cleanup_mode == CleanupMode.Move:
      if not self.settings.cleanup_move_folder:
        self.logger.warning('Not moving source file due to "cleanup_move_path" is not set')
        return
      self.MakeFolderExisting(self.settings.cleanup_move_folder)
      move_path = os.path.abspath(
          os.path.join(
              self.settings.cleanup_move_folder,
              os.path.basename(self.source_path)))
      self.logger.info('Move source file into: %s', move_path)
      if self.settings.dry_run:
        self.logger.info('DRY RUN: Not renaming %s to %s', self.source_path, move_path)
      else:
        os.rename(self.source_path, move_path)
    elif self.settings.cleanup_mode == CleanupMode.Drop:
      self.logger.info('Dropping source file: %s', self.source_path)
      if self.settings.dry_run:
        self.logger.info('DRY RUN: Not deleting %s', self.source_path)
      else:
        os.unlink(self.source_path)

  # Checks if path is either doesn't exist or is a file (hence, can be overwritten).
  def EnsureFilePathIsUnused(self, file_path):
    file_path = os.path.abspath(file_path)  # Just in case.
    if os.path.exists(file_path) and not os.path.isfile(file_path):
      self.logger.debug('Path "%s" already exists and it\'s not a file', file_path)
      raise VideoDataError('File path is already used and cannot be cleaned up')

  # Checks if folder exists, and careates it if needed.
  def MakeFolderExisting(self, folder_path):
    folder_path = os.path.abspath(folder_path)  # Just in case.
    if not os.path.exists(folder_path):
      self.logger.info('Create folder: %s', folder_path)
      if self.settings.dry_run:
        self.logger.info('DRY RUN: Not creating dir path %s', folder_path)
      else:
        os.makedirs(folder_path)
    elif not os.path.isdir(folder_path):
      self.logger.debug('Path "%s" already exists and it\'s not a folder', folder_path)
      raise VideoDataError('Folder path is already used and cannot be cleaned up')

  # Gets streams from the source and finds appropriate tracks for video, audio, and subtitles.
  def ExtractInfo(self):
    out = self.ExecuteFFPROBE(
        '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', self.source_path)
    self.metadata = json.loads(out)
    for stream in self.metadata['streams']:
      stream = Stream(self.settings, **stream)
      self.logger.debug(
          'Found stream #%s: codec_type=%s, codec_name=%s, lang=%s, forced=%s, title=%s',
          stream.index, stream.codec_type, stream.codec_name,
          stream.tags.language, stream.disposition.forced, stream.tags.title)
        
      if stream.codec_type == 'video':
        if not self.settings.save_cover_image or not self.__handle_cover(stream):
          self.__handle_video_stream(stream)
      elif stream.codec_type == 'audio':
        self.__handle_audio_stream(stream)
      elif stream.codec_type == 'subtitle':
        self.__handle_sub_stream(stream)

    if not self.audio_stream and self.settings.acodec_fallback_to_default:
      streams = map(lambda x: Stream(self.settings, **x), self.metadata['streams'])
      streams = filter(lambda x: x.codec_type == 'audio', streams)
      if len(streams) == 1 and not streams[0].disposition.default:
        stream = streams[0]
        self.logger.warning('Mark the only AUDIO stream as deafult #%s (%s)',
                            stream.index, stream.codec_name)
        stream.disposition.default = True
      for stream in streams:
        self.__handle_audio_stream(stream, fallback_to_default=True)

  def __handle_video_stream(self, stream):
    # TODO(ihsoft): Add support for multi video streams files. Handle non video codecs.
    if not self.video_stream:
      self.logger.info('Use VIDEO stream #%s (%s)', stream.index, stream.codec_name)
      self.video_stream = stream
      return True

  def __handle_audio_stream(self, stream, fallback_to_default=False):
    lang = self.GetLangCode(stream.tags.language)
    title = stream.tags.title
    matched_audio_stream = None
    if lang == self.settings.acodec_lang:
      if not self.audio_stream:
        matched_audio_stream = stream
      else:
        # If there is a DUBBING track prefer it over anything else.
        for substring in self.settings.acodec_dubbing_strings:
          if substring in title.lower():
            self.logger.info('Detected dubbing audio by title: %s (key=%s)', title, substring)
            matched_audio_stream = stream
            break

    if not matched_audio_stream and fallback_to_default and stream.disposition.default:
      self.logger.warning('Use default AUDIO stream #%s (%s)', stream.index, stream.codec_name)
      self.audio_stream = stream
      return True

    if matched_audio_stream:
      self.logger.info('Use AUDIO stream #%s (%s)', stream.index, stream.codec_name)
      self.audio_stream = matched_audio_stream
      return True

  def __handle_sub_stream(self, stream):
    lang = self.GetLangCode(stream.tags.language)
    forced = stream.disposition.forced
    title = stream.tags.title
    if not forced:
      for substring in self.settings.scodec_forced_strings:
        if substring in title.lower():
          self.logger.info('Detected forced subs by title: %s (key=%s)', title, substring)
          forced = True
          break
    if forced:
      self.logger.info('Use forced SUB #%s (%s)', stream.index, stream.codec_name)
      self.subtitles_stream = stream
      return True

  def __handle_cover(self, stream):
    filename = getattr(stream.tags, 'filename', None)
    mimetype = getattr(stream.tags, 'mimetype', None)
    if filename and mimetype and mimetype.startswith('image/') and not self.cover_stream:
      self.logger.info('Use VIDEO stream for cover #%s (%s)', stream.index, stream.codec_name) 
      self.cover_stream = stream
      return True

  def GetLangCode(self, lang_string):
    if len(lang_string) == 2:
      raise VideoDataError('Language ISO 639-1 codes are not supported: %s' % lang_string)
    return lang_string[:3].lower()

  # Runs ffmpeg with the provided arguments. If tool returns non zero result code the execution
  # fails. In case of successful run the content of STDOUT is returned.
  def ExecuteFFMPEG(self, *args):
    if self.settings.dry_run:
      self.logger.info('DRY RUN: Not executing: ffmpeg %s', ' '.join(args))
      return ''
    else:
      return self.Execute(self.settings.ffmpeg_path, *args)

  # Runs ffprobe with the provided arguments. If tool returns non zero result code the execution
  # fails. In case of successful run the content of STDOUT is returned.
  def ExecuteFFPROBE(self, *args):
    return self.Execute(self.settings.ffprobe_path, *args)

  # Executes the requested tool. If return code is not zero then this method rasies.
  # STDOUT and STDERR will be logged before existing.
  def Execute(self, *args):
    self.logger.debug('Running subprocess: %s', args)

    # In 2.7 Popen doesn't support non-ASCII arguments. To workaround localized file and
    # folder names make a executable script and run it.
    tmp_fp, tmp_cmd_name = tempfile.mkstemp(prefix='vd_', suffix='.cmd', text=True)
    os.close(tmp_fp)
    os.chmod(tmp_cmd_name, 0770)
    tmp_fp, tmp_cmd_out = tempfile.mkstemp(prefix='vd_', suffix='.out', text=True)
    os.close(tmp_fp)
    # Script must specify names in the console's encoding.
    with open(tmp_cmd_name, 'w') as fp:
      for arg in args:
        fp.write('"%s" ' % arg.encode(CONSOLE_ENCODING))
      fp.write(' > ' + tmp_cmd_out)

    p = subprocess.Popen([tmp_cmd_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _, err = p.communicate()
    with open(tmp_cmd_out, 'r') as fp:
      out = fp.read()
    os.unlink(tmp_cmd_name)
    os.unlink(tmp_cmd_out)

    if p.returncode:
      if err:
        self.logger.debug('='*80)
        self.logger.debug('STDERR:')
        self.logger.debug(err)
        self.logger.debug('='*80)
      raise VideoDataError('Cannot execute %s, return code: %s' % (args[0], p.returncode))

    return out


def ScanFiles(settings):
  global STATS

  logger = logging.getLogger('Transcode')
  logger.setLevel(logging.DEBUG)
  formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

  file_lvl = settings.get('file_loglevel', 1)
  if file_lvl != -1 and settings['file_log_name']:
    file_hndlr = logging.handlers.TimedRotatingFileHandler(
        settings['file_log_name'], when='midnight', backupCount=7)
    file_hndlr.setFormatter(formatter)
    file_hndlr.setLevel(LOG_LEVELS[file_lvl])
    logger.addHandler(file_hndlr) 

  stdout_lvl = settings.get('stdout_loglevel', 0)
  if stdout_lvl != -1:
    stdout_hndlr = logging.StreamHandler(sys.stdout)
    stdout_hndlr.setLevel(LOG_LEVELS[stdout_lvl])
    stdout_hndlr.setFormatter(formatter)
    logger.addHandler(stdout_hndlr)

  folders = settings['scan_folders']
  patterns = settings['scan_patterns']
  logger.info('Start scanning profile: %s' % settings['profile_name'])
  for folder in folders:
    folder = os.path.abspath(folder)
    logger.info('Scanning folder: %s', folder)
    for root, dirnames, filenames in os.walk(folder):
      for filename in filenames:
        if any(re.match(pattern, filename) for pattern in patterns):
          file_path = os.path.abspath(os.path.join(root, filename))
          STATS.scanned += 1
          try:
            vd = VideoData(settings, file_path, logger)
            vd.ResolveFile()
            vd.Transcode()
            vd.Cleanup()
          except VideoDataError as ex:
            STATS.errors += 1
            logger.exception('Cannot handle file: %s', file_path)
          except Exception as ex:
            logger.exception('Unexpected error while handling: %s', file_path)
            raise ex
  logger.info('End scanning')

def main(argv):
  parser = argparse.ArgumentParser(
      description='Batch AVC-MKV=>MP4 transcoder/remuxer.',
      fromfile_prefix_chars='@',
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog=textwrap.dedent('''
          Arguments can be provided via file:
            %(prog)s @input.txt

          Examples:
            %(prog)s movies.json shows.json
            %(prog)s --encoding cp866 russian_movies.json

          To find out the right console encoding try this:
            On Linux:
              ~$ echo $LANG
            On Windows:
              C:\> chcp
      '''))
  parser.add_argument(
     '--encoding', metavar='<name>', default='utf-8',
      help='''default console\'s encoding.
          Needed to deal with localized folder and file names (default: utf-8)''')
  parser.add_argument(
     'json', nargs='+', metavar='<filename>',
      type=argparse.FileType('r'),
      help='load settings from JSON file')
  opts = parser.parse_args(argv[1:])

  global CONSOLE_ENCODING
  CONSOLE_ENCODING = opts.encoding
  
  for json_fp in opts.json:
    settings = {}
    try:
      settings.update(json.load(json_fp))
    except Exception as ex:
      sys.stderr.write('Cannot load settings from "%s": %s\n' % (json_fp.name, ex))
    ScanFiles(settings)
    json_fp.close()

  logging.shutdown()
  print 'TOTAL STATS:', STATS


main(sys.argv)
