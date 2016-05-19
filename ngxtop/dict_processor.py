"""
Records and statistic processor - dict
"""
import re
import time
import calendar
from datetime import datetime
from dateutil import parser

if __package__ is None:
    from utils import to_int
    from config_parser import REGEX_LOG_FORMAT_VARIABLE, REGEX_SPECIAL_CHARS
else:
    from .utils import to_int
    from .config_parser import REGEX_LOG_FORMAT_VARIABLE, REGEX_SPECIAL_CHARS


REGEX_GET_STREAM = 'GET /live/$stream.m3u8 HTTP/1.1'


class ClientInfo(object):
    def __init__(self, name):
        self.name = name
        self.join_ts = None
        self.status = None
        self.detail = None

    @staticmethod
    def parse_time(time_str):
        return calendar.timegm(parser.parse(time_str.replace(':', ' ', 1)).utctimetuple())

    def parse_info(self, records):
        if self.join_ts is None:
            if 'time_local' not in records:
                self.join_ts = calendar.timegm(datetime.utcfromtimestamp(time.time()).utctimetuple())
            else:
                self.join_ts = self.parse_time(records['time_local'])

        if 'status' in records:
            status = to_int(records['status'])
            self.status = status

        if 'http_user_agent' in records:
            self.detail = records['http_user_agent']


class StreamInfo(object):
    def __init__(self, name):
        self.name = name
        self.in_bytes = None
        self.in_bw = None
        self.out_bytes = None
        self.out_bw = None
        self.time = None

        # request_path - ClientInfo
        self.clients = {}

    def parse_info(self, records):
        if 'remote_addr' not in records:
            return

        client = records['remote_addr']
        if client not in self.clients:
            client_info = ClientInfo(client)
            client_info.parse_info(records)
            self.clients[client] = client_info
        else:
            self.clients[client].parse_info(records)


class DictProcessor(object):
    def __init__(self):
        self.begin = False
        self.pattern = re.sub(REGEX_SPECIAL_CHARS, r'\\\1', REGEX_GET_STREAM)
        self.pattern = re.sub(REGEX_LOG_FORMAT_VARIABLE, '(?P<\\1>.*)', self.pattern)
        self.pattern = re.compile(self.pattern)

        # stream - StreamInfo
        self.streams = {}

    def process(self, records):
        self.begin = time.time()

        if 'request' not in records:
            return

        stream = 'none'
        match = self.pattern.match(records['request'])
        if match is not None:
            stream = match.groupdict()['stream']

        if stream not in self.streams:
            stream_info = StreamInfo(stream)
            stream_info.parse_info(records)
            self.streams[stream] = stream_info
        else:
            self.streams[stream].parse_info(records)

    def report(self):
        pass
