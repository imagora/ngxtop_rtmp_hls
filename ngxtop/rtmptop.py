"""
Nginx-rtmp-module stat parser.

Need to install nginx-rtmp-module first.
"""
import xml.dom.minidom
import urllib2

STAT_URL = "http://127.0.0.1:8080/stat"


def pass_for_node_value(root, node_name):
    child = root.getElementsByTagName(node_name)

    if len(child) >= 1 and child[0].firstChild:
        return child[0].firstChild.data

    return 0


class MetaInfo:
    def __init__(self):
        self.video_width = None
        self.video_height = None
        self.video_frame_rate = None
        self.video_codec = None
        self.video_profile = None
        self.video_compat = None
        self.video_level = None
        self.audio_codec = None
        self.audio_profile = None
        self.audio_channels = None
        self.audio_sample_rate = None

    def pass_info(self, meta_root):
        video_child = meta_root.getElementsByTagName('video')[0]
        self.video_width = int(pass_for_node_value(video_child, 'width'))
        self.video_height = int(pass_for_node_value(video_child, 'height'))
        self.video_frame_rate = int(pass_for_node_value(video_child, 'frame_rate'))
        self.video_codec = pass_for_node_value(video_child, 'codec')
        self.video_profile = pass_for_node_value(video_child, 'profile')
        self.video_compat = int(pass_for_node_value(video_child, 'compat'))
        self.video_level = float(pass_for_node_value(video_child, 'level'))

        audio_child = meta_root.getElementsByTagName('audio')[0]
        self.audio_codec = pass_for_node_value(audio_child, 'codec')
        self.audio_profile = pass_for_node_value(audio_child, 'profile')
        self.audio_channels = int(pass_for_node_value(audio_child, 'channels'))
        self.audio_sample_rate = int(pass_for_node_value(audio_child, 'sample_rate'))

    def print_info(self, output):
        output.append('\t\tVideo Meta: width %d, height %d, frame_rate %d, codec %s, profile %s, compat %d, level %f' %
                      (self.video_width, self.video_height, self.video_frame_rate, self.video_codec, self.video_profile,
                       self.video_compat, self.video_level))
        output.append('\t\tAudio Meta: codec %s, profile %s, channels %d, sample rate %d' %
                      (self.audio_codec, self.audio_profile, self.audio_channels, self.audio_sample_rate))


class ClientInfo:
    def __init__(self, client_root):
        self.id = int(pass_for_node_value(client_root, 'id'))
        self.address = pass_for_node_value(client_root, 'address')
        self.time = int(pass_for_node_value(client_root, 'time'))
        self.flashver = pass_for_node_value(client_root, 'flashver')

        self.pageurl = None
        self.swfurl = None

        self.dropped = int(pass_for_node_value(client_root, 'dropped'))
        self.avsync = int(pass_for_node_value(client_root, 'avsync'))
        self.timestamp = int(pass_for_node_value(client_root, 'timestamp'))

        self.is_publisher = False

    def pass_info(self, client_root):
        publish_child = client_root.getElementsByTagName('publishing')
        if publish_child.length > 0:
            self.is_publisher = True

        if not self.is_publisher:
            self.pageurl = pass_for_node_value(client_root, 'pageurl')
            self.swfurl = pass_for_node_value(client_root, 'swfurl')

    def print_info(self, output):
        if self.is_publisher:
            output.append('\t\tServer: addr %s, flashver %s' % (self.address, self.flashver))
        else:
            output.append('\t\tClient: addr %s, flashver %s, page %s, swf %s' %
                          (self.address, self.flashver, self.pageurl, self.swfurl))


class StreamInfo:
    def __init__(self, stream_root):
        self.name = pass_for_node_value(stream_root, 'name')
        self.time = int(pass_for_node_value(stream_root, 'time'))
        self.bw_in = int(pass_for_node_value(stream_root, 'bw_in'))
        self.bytes_in = int(pass_for_node_value(stream_root, 'bytes_in'))
        self.bw_out = int(pass_for_node_value(stream_root, 'bw_out'))
        self.bytes_out = int(pass_for_node_value(stream_root, 'bytes_out'))
        self.bw_audio = int(pass_for_node_value(stream_root, 'bw_audio'))
        self.bw_video = int(pass_for_node_value(stream_root, 'bw_video'))
        self.nclients = int(pass_for_node_value(stream_root, 'nclients'))

        self.meta_info = None
        self.clients = {}

    def pass_info(self, stream_root):
        meta_child = stream_root.getElementsByTagName('meta')
        if meta_child.length > 0:
            self.meta_info = MetaInfo()
            self.meta_info.pass_info(meta_child[0])

        client_child = stream_root.getElementsByTagName('client')
        for client in client_child:
            client_info = ClientInfo(client)
            client_info.pass_info(client)
            self.clients[client_info.id] = client_info

    def print_info(self, output):
        output.append('\tStream %s: time %d, bw_in %d, bytes_in %f, bw_out %d, '
                      'bytes_out %f, bw_audio %d, bs_video %d, clients %d' %
                      (self.name, self.time, self.bw_in, self.bytes_in, self.bw_out,
                       self.bytes_out, self.bw_audio, self.bw_video, self.nclients))

        output.append('\tMeta info:')
        if self.meta_info:
            self.meta_info.print_info(output)
        else:
            output.append('\t\tStream Idel')

        output.append('\t\tClient Info:')
        for client in self.clients.itervalues():
            client.print_info(output)


class NginxRtmpInfo:
    def __init__(self, root):
        self.nginx_version = pass_for_node_value(root, 'nginx_version')
        self.rtmp_version = pass_for_node_value(root, 'nginx_rtmp_version')
        self.compiler = pass_for_node_value(root, 'compiler')
        self.built = pass_for_node_value(root, 'built')
        self.pid = int(pass_for_node_value(root, 'pid'))
        self.uptime = int(pass_for_node_value(root, 'uptime'))
        self.accepted = int(pass_for_node_value(root, 'naccepted'))
        self.bw_in = int(pass_for_node_value(root, 'bw_in'))
        self.bw_out = int(pass_for_node_value(root, 'bw_out'))
        self.bytes_in = int(pass_for_node_value(root, 'bytes_in'))
        self.bytes_out = int(pass_for_node_value(root, 'bytes_out'))

        self.stream_infos = {}

    def pass_info(self, root):
        live_child = root.getElementsByTagName('server')[0].getElementsByTagName(
            'application')[0].getElementsByTagName('live')[0]
        for stream_child in live_child.getElementsByTagName('stream'):
            stream_info = StreamInfo(stream_child)
            stream_info.pass_info(stream_child)
            self.stream_infos[stream_info.name] = stream_info

    def print_info(self):
        output = list()
        output.append('Summary:')
        output.append('\tNginx version: %s, RTMP version: %s, Compiler: %s, Built: %s, PID: %d, Uptime: %ds.' %
                      (self.nginx_version, self.rtmp_version, self.compiler, self.built, self.pid, self.uptime))
        output.append('\tAccepted: %d, bw_in: %f Kbit/s, bytes_in: %02f MByte, '
                      'bw_out: %02f Kbit/s, bytes_out: %02f MByte' %
                      (self.accepted, self.bw_in / 1024.0, self.bytes_in / 1024.0 / 1024,
                       self.bw_out / 1024.0, self.bytes_out / 1024.0 / 1024))

        output.append('Detail:')
        output.append('\tStreams: %d' % len(self.stream_infos))
        for stream in self.stream_infos.itervalues():
            stream.print_info(output)

        return output


def get_rtmp_top(stat_url):
    url = STAT_URL
    if stat_url != '':
        url = stat_url

    response = urllib2.urlopen(url)
    dom = xml.dom.minidom.parseString(response.read())
    root = dom.documentElement

    rtmp_info = NginxRtmpInfo(root)
    rtmp_info.pass_info(root)

    return rtmp_info
