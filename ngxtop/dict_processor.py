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
REGEX_GET_STREAM_TS = 'GET /live/$stream-$frag.ts HTTP/1.1'
TOTAL_SUMMARY_INFO = '\tClients: %d OutMBytes: %d OutKBytes/s %d Time %ds\n'
STREAM_SUMMARY_INFO = '\tStream: %s OutMBytes: %d OutKBytes/s %d Time %ds\n'
CLIENT_SUMMARY_INFO = '\t\tClient: %s Info: %s Time %ds\n'


class ClientInfo(object):
    def __init__(self, name):
        self.name = name
        self.join_ts = None
        self.status = None
        self.detail = ''

    @staticmethod
    def parse_time(time_str):
        return calendar.timegm(parser.parse(time_str.replace(':', ' ', 1)).utctimetuple())

    def parse_info(self, records):
        if self.join_ts is None:
            if 'time' in records:
                self.join_ts = calendar.timegm(datetime.utcfromtimestamp(time.time()).utctimetuple()) - \
                               to_int(records['time']) * 1000
            elif 'time_local' in records:
                self.join_ts = self.parse_time(records['time_local'])
            else:
                self.join_ts = calendar.timegm(datetime.utcfromtimestamp(time.time()).utctimetuple())

        if 'status' in records:
            status = to_int(records['status'])
            self.status = status

        if 'http_user_agent' in records:
            self.detail = records['http_user_agent']


class StreamInfo(object):
    def __init__(self, name):
        self.name = name
        self.in_bytes = 0
        self.in_bw = 0
        self.out_bytes = 0
        self.out_bw = 0
        self.start_ts = 0

        # request_path - ClientInfo
        self.clients = {}

    def parse_info(self, records):
        if 'remote_addr' not in records:
            return

        client = records['remote_addr']
        client_info = None
        if client not in self.clients:
            client_info = ClientInfo(client)
            client_info.parse_info(records)
            self.clients[client] = client_info
        else:
            client_info = self.clients[client]
            client_info.parse_info(records)

        if self.start_ts == 0 or self.start_ts > client_info.join_ts:
            self.start_ts = client_info.join_ts

        duration = calendar.timegm(datetime.utcfromtimestamp(time.time()).utctimetuple()) - self.clients[client].join_ts

        if 'in_bytes' in records:
            self.in_bytes += to_int(records['in_bytes'])

        if 'in_bw' in records:
            self.in_bw = to_int(records['in_bw'])

        if 'out_bytes' in records:
            self.out_bytes += to_int(records['out_bytes'])
        elif 'bytes_sent' in records:
            self.out_bytes += to_int(records['body_bytes_sent'])

        if 'out_bw' in records:
            self.out_bw = to_int(records['out_bw'])
        else:
            if duration > 0:
                self.out_bw = self.out_bytes / duration * 1000 / 1024.0
            else:
                self.out_bw = self.out_bytes / 1024.0


class DictProcessor(object):
    def __init__(self):
        self.begin = False
        self.patterns = []

        pattern = re.sub(REGEX_SPECIAL_CHARS, r'\\\1', REGEX_GET_STREAM)
        pattern = re.sub(REGEX_LOG_FORMAT_VARIABLE, '(?P<\\1>.*)', pattern)
        pattern = re.compile(pattern)
        self.patterns.append(pattern)
        pattern = re.sub(REGEX_SPECIAL_CHARS, r'\\\1', REGEX_GET_STREAM_TS)
        pattern = re.sub(REGEX_LOG_FORMAT_VARIABLE, '(?P<\\1>.*)', pattern)
        pattern = re.compile(pattern)
        self.patterns.append(pattern)

        # stream - StreamInfo
        self.streams = {}

    def process(self, records):
        self.begin = time.time()

        for record in records:
            if 'request' not in record:
                return

            stream = 'none'
            match = None
            for pattern in self.patterns:
                match = pattern.match(record['request'])
                if match is not None:
                    break

            if match is not None:
                stream = match.groupdict()['stream']
            else:
                stream = record['request']

            if stream not in self.streams:
                stream_info = StreamInfo(stream)
                stream_info.parse_info(record)
                self.streams[stream] = stream_info
            else:
                self.streams[stream].parse_info(record)

    def report(self):
        output = 'Summary:\n'

        client_cnt = out_bytes = out_bw = run_time = 0
        stream_output = ''
        for stream in self.streams.itervalues():
            client_cnt += len(stream.clients)
            out_bytes += stream.out_bytes
            out_bw += stream.out_bw
            if run_time != 0:
                run_time = min(stream.start_ts, run_time)
            else:
                run_time = stream.start_ts

            stream_output += STREAM_SUMMARY_INFO % (
                stream.name, stream.out_bytes / 1024.0 / 1024.0, stream.out_bw, (calendar.timegm(
                    datetime.utcfromtimestamp(time.time()).utctimetuple()) - stream.start_ts) / 1000)

            for client in stream.clients.itervalues():
                stream_output += CLIENT_SUMMARY_INFO % (client.name, client.detail, (calendar.timegm(
                    datetime.utcfromtimestamp(time.time()).utctimetuple()) - client.join_ts) / 1000)

        run_time = (calendar.timegm(datetime.utcfromtimestamp(time.time()).utctimetuple()) - run_time) / 1000
        output += TOTAL_SUMMARY_INFO % (client_cnt, out_bytes / 1024.0 / 1024.0, out_bw, run_time)
        output += '\nDetail:\n'
        output += stream_output
        return output
