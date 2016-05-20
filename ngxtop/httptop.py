"""
Nginx access.log parser.
"""
import os
import sys
import time
import logging

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

if __package__ is None:
    from config_parser import detect_log_config, build_pattern
    from utils import error_exit, to_float, to_int
else:
    from .config_parser import detect_log_config, build_pattern
    from .utils import error_exit, to_float, to_int


class NginxHttpInfo(object):
    def __init__(self, arguments):
        self.arguments = arguments
        self.processor = None
        self.access_log = None
        self.pattern = None

    @staticmethod
    def map_field(field, func, dict_sequence):
        """
        Apply given function to value of given key in every dictionary in sequence and
        set the result as new value for that key.
        :param field:
        :param func:
        :param dict_sequence:
        """
        for item in dict_sequence:
            try:
                item[field] = func(item.get(field, None))
                yield item
            except ValueError:
                pass

    @staticmethod
    def add_field(field, func, dict_sequence):
        """
        Apply given function to the record and store result in given field of current record.
        Do nothing if record already contains given field.
        :param field:
        :param func:
        :param dict_sequence:
        """
        for item in dict_sequence:
            if field not in item:
                item[field] = func(item)
            yield item

    @staticmethod
    def parse_request_path(record):
        if 'request_uri' in record:
            uri = record['request_uri']
        elif 'request' in record:
            uri = ' '.join(record['request'].split(' ')[1:-1])
        else:
            uri = None
        return urlparse.urlparse(uri).path if uri else None

    @staticmethod
    def parse_status_type(record):
        return record['status'] // 100 if 'status' in record else None

    def set_processor(self, processor):
        self.processor = processor

    def parse_log(self, lines):
        matches = (self.pattern.match(l) for l in lines)
        records = (m.groupdict() for m in matches if m is not None)

        records = self.map_field('status', to_int, records)
        records = self.add_field('status_type', self.parse_status_type, records)
        records = self.add_field('bytes_sent', lambda r: r['body_bytes_sent'], records)
        records = self.map_field('bytes_sent', to_int, records)
        records = self.map_field('request_time', to_float, records)
        records = self.add_field('request_path', self.parse_request_path, records)
        return records

    def get_access_log(self):
        """
        Get nginx access.log file path
        :return: access.log file path and log format
        """
        if self.access_log is not None:
            return self.access_log

        self.access_log = self.arguments['--access-log']
        log_format = self.arguments['--log-format']
        if self.access_log is None and not sys.stdin.isatty():
            # assume logs can be fetched directly from stdin when piped
            self.access_log = 'stdin'
        if self.access_log is None:
            self.access_log, log_format = detect_log_config(self.arguments)

        logging.info('access_log: %s', self.access_log)
        logging.info('log_format: %s', log_format)
        if self.access_log != 'stdin' and not os.path.exists(self.access_log):
            error_exit('access log file "%s" does not exist' % self.access_log)
        return self.access_log, log_format

    def follow(self):
        """
        Follow a given file and yield new lines when they are available, like `tail -f`.
        :return: new lines appended
        """
        with open(self.access_log) as f:
            f.seek(0, 2)  # seek to eof
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.1)  # sleep briefly before trying again
                    continue
                yield line

    def build_source(self):
        """
        Load lines to parse
        :return: loaded lines
        """
        # constructing log source
        if self.access_log == 'stdin':
            lines = sys.stdin
        elif self.arguments['--no-follow']:
            lines = open(self.access_log)
        else:
            lines = self.follow()
        return lines

    def process_log(self, lines):
        pre_filer_exp = self.arguments['--pre-filter']
        if pre_filer_exp:
            lines = (line for line in lines if eval(pre_filer_exp, {}, dict(line=line)))

        records = self.parse_log(lines)

        filter_exp = self.arguments['--filter']
        if filter_exp:
            records = (r for r in records if eval(filter_exp, {}, r))

        self.processor.process(records)
        print(self.processor.report())  # this will only run when start in --no-follow mode

    def parse_info(self):
        if self.access_log is None:
            self.get_access_log()

        if self.pattern is None:
            self.pattern = build_pattern(self.arguments['--log-format'])

        lines = self.build_source()
        self.process_log(lines)
