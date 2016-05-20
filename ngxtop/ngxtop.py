"""ngxtop - ad-hoc query for nginx access log.

Usage:
    ngxtop [options]
    ngxtop [options] (print|top|avg|sum) <var> ...
    ngxtop info
    ngxtop [options] query <query> ...

Options:
    -l <file>, --access-log <file>  access log file to parse.
    -r <url>, --rtmp-stat-url <url>  rtmp stat url to parse.
    -f <format>, --log-format <format>  log format as specify in log_format directive. [default: combined]
    --no-follow  ngxtop default behavior is to ignore current lines in log
                     and only watch for new lines as they are written to the access log.
                     Use this flag to tell ngxtop to process the current content of the access log instead.
    -t <seconds>, --interval <seconds>  report interval when running in follow mode [default: 2.0]
    -s <samples>, --samples <samples>  Use logging mode and display samples, even if standard output is a terminal.

    -g <var>, --group-by <var>  group by variable [default: request_path]
    -w <var>, --having <expr>  having clause [default: 1]
    -o <var>, --order-by <var>  order of output for default query [default: count]
    -n <number>, --limit <number>  limit the number of records included in report for top command [default: 10]
    -a <exp> ..., --a <exp> ...  add exp (must be aggregation exp: sum, avg, min, max, etc.) into output

    -v, --verbose  more verbose output
    -d, --debug  print every line and parsed record
    -h, --help  print this help message.
    --version  print version information.

    Advanced / experimental options:
    -c <file>, --config <file>  allow ngxtop to parse nginx config file for log format and location.
    -i <filter-expression>, --filter <filter-expression>  filter in, records satisfied given expression are processed.
    -p <filter-expression>, --pre-filter <filter-expression> in-filter expression to check in pre-parsing phase.

Examples:
    All examples read nginx config file for access log location and format.
    If you want to specify the access log file and / or log format, use the -f and -a options.

    "top" like view of nginx requests
    $ ngxtop

    Top 10 requested path with status 404:
    $ ngxtop top request_path --filter 'status == 404'

    Top 10 requests with highest total bytes sent
    $ ngxtop --order-by 'avg(bytes_sent) * count'

    Top 10 remote address, e.g., who's hitting you the most
    $ ngxtop --group-by remote_addr

    Print requests with 4xx or 5xx status, together with status and http referer
    $ ngxtop -i 'status >= 400' print request status http_referer

    Average body bytes sent of 200 responses of requested path begin with 'foo':
    $ ngxtop avg bytes_sent --filter 'status == 200 and request_path.startswith("foo")'

    Analyze apache access log from remote machine using 'common' log format
    $ ssh remote tail -f /var/log/apache2/access.log | ngxtop -f common
"""
from __future__ import print_function
import atexit
import curses
import logging
import sys
import signal

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from docopt import docopt

if __name__ == '__main__' and __package__ is None:
    from config_parser import detect_config_path, extract_variables
    from sql_processor import SQLProcessor
    from dict_processor import DictProcessor
    from rtmptop import NginxRtmpInfo
    from httptop import NginxHttpInfo
else:
    from .config_parser import detect_config_path, extract_variables
    from .sql_processor import SQLProcessor
    from .dict_processor import DictProcessor
    from .rtmptop import NginxRtmpInfo
    from .httptop import NginxHttpInfo

"""
* RTMP&HLS HLS

Summary:
    Clients: - OutMBytes: - OutKBytes/s - Time -
Detail:
    HLS Stream: - OutMBytes: - OutKBytes/s - Time -
        Client: - URL: - Info: - Time -
        Client: - URL: - Info: - Time -
        Client: - URL: - Info: - Time -

    RTMP Stream: -
    Stream -: time -, bw_in -, bytes_in -, bw_out -, bytes_out -, bw_audio -, bw_video -, clients -
    Meta info:
        Video Meta: width -, height -, frame_rate -, codec -, profile -, compat -, level -
        Audio Meta: codec -, profile -, channels -, sample rate -
    Client Info:
        Server: addr -, flashver -
        Client: addr -, flashver -, page -, swf -

* RTMP

Summary:
    Nginx version: -, RTMP version: -, Compiler: -, Built: -, PID: -, Uptime: -.
    Accepted: -, bw_in: -Kbit/s, bytes_in: -MByte, bw_out: -Kbit/s, bytes_out: -MByte
Detail:
    Streams: -
    Stream -: time -, bw_in -, bytes_in -, bw_out -, bytes_out -, bw_audio -, bw_video -, clients -
    Meta info:
        Video Meta: width -, height -, frame_rate -, codec -, profile -, compat -, level -
        Audio Meta: codec -, profile -, channels -, sample rate -
    Client Info:
        Server: addr -, flashver -
        Client: addr -, flashver -, page -, swf -
"""
DEFAULT_QUERIES = [
    ('Summary:',
     '''SELECT
       count(1)                                    AS count,
       avg(bytes_sent)                             AS avg_bytes_sent,
       count(CASE WHEN status_type = 2 THEN 1 END) AS '2xx',
       count(CASE WHEN status_type = 3 THEN 1 END) AS '3xx',
       count(CASE WHEN status_type = 4 THEN 1 END) AS '4xx',
       count(CASE WHEN status_type = 5 THEN 1 END) AS '5xx'
     FROM log
     ORDER BY %(--order-by)s DESC
     LIMIT %(--limit)s
     '''),

    ('Detailed:',
     '''SELECT
       %(--group-by)s,
       count(1)                                    AS count,
       avg(bytes_sent)                             AS avg_bytes_sent,
       count(CASE WHEN status_type = 2 THEN 1 END) AS '2xx',
       count(CASE WHEN status_type = 3 THEN 1 END) AS '3xx',
       count(CASE WHEN status_type = 4 THEN 1 END) AS '4xx',
       count(CASE WHEN status_type = 5 THEN 1 END) AS '5xx'
     FROM log
     GROUP BY %(--group-by)s
     HAVING %(--having)s
     ORDER BY %(--order-by)s DESC
     LIMIT %(--limit)s''')
]
DEFAULT_FIELDS = set(['stream', 'request_path', 'join_ts', 'time', 'status_type', 'detail'])
LOGGING_SAMPLES = None


class NginxTop(object):
    def __init__(self, arguments):
        self.sql_processor = None
        self.arguments = arguments
        self.http_top = NginxHttpInfo(arguments)
        self.rtmp_top = NginxRtmpInfo(arguments)
        self.rtmp_stat_url = arguments['--rtmp-stat-url']
        self.logging_samples = arguments['--samples']
        if self.logging_samples is not None:
            self.logging_samples = int(self.logging_samples)
        self.scr = curses.initscr()
        atexit.register(curses.endwin)

    def build_processor(self):
        if self.sql_processor is not None:
            return

        self.sql_processor = DictProcessor()
        self.http_top.set_processor(self.sql_processor)
        self.rtmp_top.set_processor(self.sql_processor)
        return

        fields = self.arguments['<var>']
        if self.arguments['print']:
            label = ', '.join(fields) + ':'
            selections = ', '.join(fields)
            query = 'select %s from log group by %s' % (selections, selections)
            report_queries = [(label, query)]
        elif self.arguments['top']:
            limit = int(self.arguments['--limit'])
            report_queries = []
            for var in fields:
                label = 'top %s' % var
                query = 'select %s, count(1) as count from log group by %s order by count desc limit %d' % (var, var, limit)
                report_queries.append((label, query))
        elif self.arguments['avg']:
            label = 'average %s' % fields
            selections = ', '.join('avg(%s)' % var for var in fields)
            query = 'select %s from log' % selections
            report_queries = [(label, query)]
        elif self.arguments['sum']:
            label = 'sum %s' % fields
            selections = ', '.join('sum(%s)' % var for var in fields)
            query = 'select %s from log' % selections
            report_queries = [(label, query)]
        elif self.arguments['query']:
            report_queries = self.arguments['<query>']
            fields = self.arguments['<fields>']
        else:
            report_queries = [(name, query % self.arguments) for name, query in DEFAULT_QUERIES]
            fields = DEFAULT_FIELDS.union(set([self.arguments['--group-by']]))

        for label, query in report_queries:
            logging.info('query for "%s":\n %s', label, query)

        processor_fields = []
        for field in fields:
            processor_fields.extend(field.split(','))

        self.sql_processor = SQLProcessor(report_queries, processor_fields)
        self.http_top.set_processor(self.sql_processor)
        self.rtmp_top.set_processor(self.sql_processor)

    def print_report(self, sig, frame):
        if self.rtmp_stat_url is not None:
            self.rtmp_top.parse_info()
            # output = output + '\n\n' + '\n'.join(self.rtmp_top.print_info())

        output = self.sql_processor.report()

        if self.logging_samples is None:
            self.scr.erase()

            try:
                self.scr.addstr(output)
            except curses.error:
                pass

            self.scr.refresh()
        else:
            print(output)
            self.logging_samples -= 1
            if self.logging_samples == 0:
                sys.exit(0)

    def setup_reporter(self):
        if self.arguments['--no-follow']:
            return

        signal.signal(signal.SIGALRM, self.print_report)
        interval = float(self.arguments['--interval'])
        signal.setitimer(signal.ITIMER_REAL, 0.1, interval)

    def run(self):
        access_log, log_format = self.http_top.get_access_log()
        if self.arguments['info']:
            print('nginx configuration file:\n ', detect_config_path())
            print('nginx rtmp stat url:\n ', self.rtmp_top.get_rtmp_url())
            print('access log file:\n ', access_log)
            print('access log format:\n ', log_format)
            print('available variables:\n ', ', '.join(sorted(extract_variables(log_format))))
            return

        self.build_processor()
        self.setup_reporter()
        self.http_top.parse_info()


def main():
    args = docopt(__doc__, version='xstat 0.1')

    log_level = logging.WARNING
    if args['--verbose']:
        log_level = logging.INFO
    if args['--debug']:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')
    logging.debug('arguments:\n%s', args)

    try:
        NginxTop(args).run()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == '__main__':
    main()
