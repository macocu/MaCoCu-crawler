#===============================================================================
#   MaCoCu crawler
#   Based on SpiderLing crawler by Vít Suchomel
#   https://macocu.eu/
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#===============================================================================

#!TODO Content-Encoding: gzip

import io, re, socket, ssl, urllib.parse
from errno import EINPROGRESS
from os.path import dirname
from posixpath import normpath
import util
from util import config

import logging

HTTP_REDIRECT_CODES = (301, 302, 303, 307)
ROBOT_PATH = '/robots.txt'
CHUNK_SIZE = 100*1024
NO_HOST = 'NO_HOST'
#try writing to an open socket in CONN_RW_READY_PERIOD windows up to MAX_CONN_READS/WRITES times
CONN_RW_READY_PERIOD = 3
MAX_CONN_WRITES = 5
MAX_CONN_READS = 20
MAX_PATH_LEN = 500

URL_RE = re.compile('https?://.+\..+', re.I)
#accepted content types --> file types
CONTENT_TYPES = {
    'text/html':                                                               'html',
    'text/plain':                                                              'txt',
    'application/msword':                                                      'doc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
    'application/vnd.oasis.opendocument.text':                                 'odt',
    'application/pdf':                                                         'pdf',
    'application/postscript':                                                  'ps',
}
BAD_FILE_EXTENSIONS = '7z|ai|aiff|apk|asf|avi|bin|bmp|bz2|c|com|css|deb|djvu|dvi|eot|eps|exe|f4v' \
    '|flv|gif|gz|h|h263|h264|h265|ico|iso|jar|jpg|jpeg|js|m4v|mid|mkv|mng|mov|mp2|mp3|mp4|mpeg' \
    '|mpg|msi|ods|ogg|ogv|pas|phar|png|ppt|pptx|psd|qt|ra|ram|rm|rpm|rtf|sdd|sdw|sh|sit|svg' \
    '|swf|sxc|sxi|sxw|tar|tex|tgz|tif|tiff|ttf|wav|webm|wma|wmf|wmv|woff|xcf|xls|xlsx|xml|xz|zip'
BAD_FILE_EXTENSIONS_RE = re.compile('\.(?:%s)$' % BAD_FILE_EXTENSIONS, re.I)
BIN_FILE_EXTENSIONS_RE = re.compile('\.(?:doc|docx|odt|pdf|ps)$', re.I)
#domain list regexps – match domains ending with parts matched by items in the lists
def get_domain_list_re(domain_list_path):
    if domain_list_path:
        comment_re = re.compile('#.*')
        domain_list = set()
        with io.open(domain_list_path, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
                newline='\n') as domain_list_file:
            for domain_s in domain_list_file.read().split('\n'):
                domain_s = comment_re.sub('', domain_s).strip()
                if domain_s:
                    domain_list.add(domain_s)
        return re.compile('(?:^|\.)(?:%s)$' % '|'.join(domain_list), re.UNICODE | re.IGNORECASE)
domain_blacklist_re = get_domain_list_re(config.DOMAIN_BLACKLIST_PATH)
domain_whitelist_re = get_domain_list_re(config.DOMAIN_WHITELIST_PATH)
if config.DOMAIN_BLACKLIST_EXACT_PATH:
    with io.open(config.DOMAIN_BLACKLIST_EXACT_PATH, mode='rt', encoding='utf-8',
            buffering=util.BUFSIZE, newline='\n') as domain_list_file:
        domain_blacklist_exact = set(domain_list_file.read().rstrip().split('\n'))
TLD_WHITELIST_RE = re.compile(config.TLD_WHITELIST)
TLD_NATIVE_RE = re.compile(config.TLD_NATIVE)
TLD_BLACKLIST_RE = re.compile(config.TLD_BLACKLIST) if config.TLD_BLACKLIST else None
content_length_bs_re = re.compile(b'content-length:\s*(\d*)')
space_re = re.compile('\s+')

if config.SSL_ENABLED:
    ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
    ssl_context.verify_mode = ssl.CERT_NONE #no certificate verification
    ssl_context.check_hostname = False
    ssl_context.load_default_certs()

def get_ip(hostname):
    #get IPv4 address
    try:
        return socket.gethostbyname(b'%s.' % hostname.encode('utf-8')) #blocking
    except OSError as e:
        logging.debug('OSError %s in get_ip(%s): %s' % (e.errno, hostname, e.strerror))
        return NO_HOST
    except Exception as e:
        logging.warning('Exception in get_ip: %s' % e)
        return NO_HOST

def url_strip(url):
    return url.replace('%20', ' ').strip().replace(' ', '%20')

def url_split(url):
    parts = urllib.parse.urlsplit(url)
    path, query = url_strip(parts.path), url_strip(parts.query)
    if query:
        full_path = ('%s?%s' % (path, query))
    else:
        full_path = path if path else '/'
    return (parts.scheme, url_strip(parts.netloc), full_path)

def url_join(scheme, host, full_path):
    if not full_path:
        full_path = '/'
    elif full_path[0] != '/':
        full_path = '/%s' % full_path
    return '%s://%s%s' % (scheme, host, full_path)

def url_join_rel_norm(src_url, target_url):
    #resolve relative paths, join with base url, normalise (no trailing slash, no fragment)
    src_parts = urllib.parse.urlsplit(src_url.replace(' ', ''), allow_fragments=False)
    target_parts = urllib.parse.urlsplit(target_url.replace(' ', ''), allow_fragments=False)
    result_scheme = target_parts.scheme if target_parts.scheme else src_parts.scheme
    result_netloc = target_parts.netloc if target_parts.netloc else src_parts.netloc
    if target_parts.path:
        if not target_parts.scheme and not target_parts.netloc and \
                not target_parts.path[0] == '/': #relative path
            src_path_dir = dirname(src_parts.path).rstrip('/')
            result_path = '%s/%s' % (src_path_dir, target_parts.path.lstrip('/'))
        else:
            result_path = target_parts.path
        result_path = normpath(result_path)
        if target_parts.path.endswith('/') and not result_path.endswith('/'):
            result_path = '%s/' % result_path #keep a single trailing slash
    else:
        result_path = '/'
    if result_netloc and result_netloc[-1] == '.':
        result_netloc = result_netloc[:-1]
    return urllib.parse.urlunsplit(
        (result_scheme, result_netloc, result_path, target_parts.query, ''))

BLOGSPOT_TLD_RE = re.compile('blogspot\.../')
def clean_url_display(scheme, host, path):
    if config.DECODE_IDNA_HOSTNAMES and (host.startswith('xn--') or '.xn--' in host):
        try:
            host = host.encode('utf-8').decode('idna')
        except UnicodeError as e:
            logging.debug('Cannot decode "%s" from IDNA: %s' % (host, e))
    url = url_join(scheme.lower(), host.lower(), path)
    url = util.clean_xml_val(space_re.sub('', url)).replace('[', '%5B').replace(
        ']', '%5D').replace('\\', '%5C')
    url = BLOGSPOT_TLD_RE.sub('blogspot.com/', url) #blogspot.cz --> blogspot.com
    return url[:MAX_PATH_LEN] #truncate

def _check_url_host(host, src_host, empty_page=False):
    if len(host) > config.MAX_HOSTNAME_LEN:
        return 'too-long'
    #hosts containing repetitive parts
    split_ok = True
    if host[:8] == 'www.www.':
        split_ok = False
    else:
        split_parts = host.split('.')
        if len(set(split_parts)) + 3 < len(split_parts):
            split_ok = False
    if not split_ok:
        return 'repetitive'
    #empty or unwanted domains:
    if not host:
        return 'empty-domain'
    if config.TLD_BLACKLIST and TLD_BLACKLIST_RE.search(host):
        if config.DOMAIN_WHITELIST_PATH:
            if not domain_whitelist_re.search(host):
                return 'blacklisted-TLD+not-white-domain'
        else:
            return 'blacklisted-TLD'
    if config.DOMAIN_BLACKLIST_EXACT_PATH and host in domain_blacklist_exact \
            or config.DOMAIN_BLACKLIST_PATH and domain_blacklist_re.search(host):
        return 'blacklisted-domain'
    #check top level domain and allowed sites
    if config.TLD_WHITELIST and not TLD_WHITELIST_RE.search(host):
        if config.DOMAIN_WHITELIST_PATH:
            if not domain_whitelist_re.search(host):
                return 'unaccepted-TLD+not-white-domain'
        else:
            return 'unaccepted-TLD'
    #check conditions for links from empty web pages
    if empty_page:
        if host == src_host:
            if not config.EXTRACT_EMPTY_PAGE_INTERNAL_LINKS:
                return 'empty-page'
        elif not config.EXTRACT_EMPTY_PAGE_EXTERNAL_LINKS or not TLD_NATIVE_RE.search(host) \
                and (not config.DOMAIN_WHITELIST_PATH or not domain_whitelist_re.search(host)):
            return 'empty-page'

def _check_url_path(path):
    if len(path) > MAX_PATH_LEN or path.count('/') > 30 or path.count('.') > 30:
        return 'too-long'
    #paths containing repetitive parts
    split_ok = True
    for split_char in '/&.':
        split_parts = path.split(split_char)
        if len(set(split_parts)) + 4 < len(split_parts):
            split_ok = False
            break
    if not split_ok:
        return 'repetitive'
    #unwanted file extensions
    if BAD_FILE_EXTENSIONS_RE.search(path):
        return 'bad-file-type'
    #binary (non txt/html) files
    if not config.CONVERSION_ENABLED and BIN_FILE_EXTENSIONS_RE.search(path):
        return 'binary-file'

def url_split_and_check_parts(url, src_host=None, empty_page=False):
    url = util.space_re.sub('', url)
    if not URL_RE.match(url) or len(url) < 12:
        return (None, 'url-invalid-or-short')
    #split to parts
    try:
        link_scheme, link_host, link_path = url_split(url)
    except ValueError:
        logging.warning('url_split fail %s' % url)
        return (None, 'url-unparseable')
    #check scheme
    if link_scheme not in ('http', 'https'):
        return (None, 'scheme-unsupported')
    #check host and path
    bad_host_reason = _check_url_host(link_host, src_host, empty_page)
    if bad_host_reason:
        return (None, 'host-%s' % bad_host_reason)
    bad_path_reason = _check_url_path(link_path)
    if bad_path_reason:
        return (None, 'path-%s' % bad_path_reason)
    return ((link_scheme, link_host, link_path), None)

content_type_re = re.compile(b'Content-Type:\s*([^;\s]+)', re.I)
def extract_file_type(http_header_bs):
    try:
        return CONTENT_TYPES[content_type_re.search(http_header_bs).group(1).decode('utf-8')]
    except (AttributeError, UnicodeDecodeError, KeyError):
        return None

last_modified_re = re.compile(
    'Last-Modified:.*(\d\d) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (20\d\d)', re.I)
mon_str_to_num = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
    'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
def extract_last_modified(http_header_bs):
    try:
        day, mon_str, year = last_modified_re.search(http_header_bs.decode('utf-8')).groups()
        return '%s-%s-%s' % (year, mon_str_to_num[mon_str], day)
    except (UnicodeDecodeError, AttributeError, KeyError):
        return None

class HTTPException(Exception):
    pass
class IncorrectDataChunkError(HTTPException):
    pass
class ConnectionException(Exception):
    pass

class IncorrectResponseError(HTTPException):
    def __init__(self, message, remote_fault=False):
        super(IncorrectResponseError, self).__init__(message)
        #a flag saying we are 90 % sure the error is caused by the remote
        # communication partner, not by an inability of this library
        self._remote_fault = remote_fault

    def is_remote_fault(self):
        return self._remote_fault

class RedirectException(HTTPException):
    def __init__(self, redirect_url):
        super(Exception, self).__init__()
        self._redirect_url = redirect_url

    def get_redirect_url(self):
        return self._redirect_url

    def __unicode__(self):
        return 'Redirect to %s' % self.get_redirect_url()

class UrlParts(object):
    __slots__ = ('url', 'scheme', 'host', 'path', 'ip', 'src_scheme', 'src_host', 'redir_count')

    def __init__(self, url, scheme, host, path, ip, src_scheme=None, src_host=None, redir_count=0):
        self.scheme = scheme if config.SSL_ENABLED else 'http'
        self.url, self.host, self.path, self.ip = url, host, path, ip
        self.src_scheme, self.src_host, self.redir_count = src_scheme, src_host, redir_count

    def is_robot(self):
        return self.src_host is not None

class Connection(object):
    __slots__ = ('url_parts', '_socket', '_request_msg_bs', '_data_read_bs',
        '_last_action_time', '_socket_ready_time', '_socket_try_count', '_info_msgs')

    def __init__(self, url_parts):
        self.url_parts = url_parts
        #create a socket, add the SSL/TLS layer in case of https
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if self.url_parts.scheme == 'https':
                self._socket = ssl_context.wrap_socket(self._socket, server_hostname=url_parts.host)
        except Exception as e:
            try:
                logging.warning('Ignoring exception in ssl_context.wrap_socket with host %s: %s' % (url_parts.host, e))
            except Exception as e:
                logging.warning('Ignoring exception in ssl_context.wrap_socket: %s' % e)
            raise ConnectionException()
        self._request_msg_bs = (
            'GET %s HTTP/1.1\r\n'
            'Host: %s\r\n'
            'User-Agent: %s\r\n'
            'Connection: close\r\n\r\n' %
            (url_parts.path, url_parts.host, config.USER_AGENT)).encode('utf-8')
        self._data_read_bs = b''
        self._last_action_time = None
        self._socket_ready_time = None
        self._socket_try_count = 0
        self._info_msgs = []

    def connect(self, now_time=None):
        self._last_action_time = now_time or util.now()
        self._socket.setblocking(False) #non-blocking socket
        logging.debug('CONN %s %s %s %s' %
            (self.url_parts.ip, self.url_parts.scheme, self.url_parts.host,
            self.url_parts.path[:MAX_PATH_LEN]))
        port = 443 if self.url_parts.scheme == 'https' else 80
        try:
            #connect to an IP (or using a hostname if the IP is not known at the cost of a slow DNS)
            self._socket.connect_ex((self.url_parts.ip, port))
        except OSError as e:
            if e.errno != EINPROGRESS: #115 => ok (operation in progress)
                self._info_msgs.append('OSError %s in connect: %s' % (e.errno, e.strerror))
                raise ConnectionException()
        except Exception as e:
            self._info_msgs.append('Exception in connect: %s => socket not connected' % e)
            raise ConnectionException()
        return self._socket.fileno()

    def handle_write(self):
        #stop sending if could not send all in several attempts (~0.7 % of cases)
        if self._socket_try_count > MAX_CONN_WRITES and self._request_msg_bs:
            self._info_msgs.append('could not send all data')
            self.close()
            raise ConnectionException()
        #check connection errors (the socket has to be connected)
        try:
            socket_error = self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        except OSError as e:
            socket_error = '%s %s' % (e.errno, e.strerror)
        if socket_error:
            self._info_msgs.append('OSError %s in handle_write (conn err check)' % socket_error)
            self.close()
            raise ConnectionException()
        #send message
        try:
            sent_size = self._socket.send(self._request_msg_bs)
        except (ssl.SSLWantWriteError, ssl.SSLWantReadError):
            pass #SSL layer not ready for writing/reading => try again later
        except OSError as e:
            if e.errno == 0:
                self._info_msgs.append('OSError %s in handle_write ignored' % e.errno)
            else:
                self._info_msgs.append('OSError %s in handle_write: %s' % (e.errno, e.strerror))
                self.close()
                raise ConnectionException()
        except Exception as e:
            self._info_msgs.append('Exception in handle_write: %s => socket closed' % e)
            self.close()
            raise ConnectionException()
        else:
            self._last_action_time = util.now()
            self._request_msg_bs = self._request_msg_bs[sent_size:]
            if not self._request_msg_bs:
                self._socket_try_count = 0
                return True #all sent
        if not self._socket_ready_time or \
                util.seconds_left(self._socket_ready_time, CONN_RW_READY_PERIOD) <= 0:
            self._socket_try_count += 1
            self._socket_ready_time = util.now()
        return False #not finished yet

    def handle_read(self):
        #raises ConnectionException, util.CheckFailException, IncorrectResponseError,
        # IncorrectDataChunkError, RedirectException
        #stop receiving if could not receive all in several attempts
        if self._socket_try_count > MAX_CONN_READS:
            self._info_msgs.append('could not receive all data')
            self.close()
            raise ConnectionException()
        #try reading data from the socket
        header_bs, body_bs = b'', b''
        try:
            data_bs = self._socket.recv(CHUNK_SIZE) #encoded
        except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
            result_keep_conn = True #SSL layer not ready for reading/writing => try again later
        except OSError as e:
            #Connection reset by peer, Connection timed out,...
            self._info_msgs.append('OSError %s in handle_read: %s' % (e.errno, e.strerror))
            self.close()
            raise ConnectionException()
        except Exception as e:
            self._info_msgs.append('Exception in handle_read: %s => socket closed' % e)
            self.close()
            raise ConnectionException()
        else:
            if data_bs:
                #some data read this time
                self._last_action_time = util.now()
                #examine the HTTP header for Content-Length
                if not self._data_read_bs:
                    #first data chunk read, assuming the HTTP header is here
                    s = content_length_bs_re.search(data_bs, re.IGNORECASE) #encoded
                    if s and s.group(1):
                        data_len = int(s.group(1))
                        if data_len > config.DOC_SIZE_MAX:
                            self._info_msgs.append('Check in handle_read: content size too high')
                            self.close()
                            raise ConnectionException()
                #add read data
                self._data_read_bs += data_bs
                #too much data to read => close the connection
                if len(self._data_read_bs) > config.DOC_SIZE_MAX:
                    self._info_msgs.append('Check in handle_read: content size too high')
                    self.close()
                    raise ConnectionException()
                #read ok but keep the connection to allow future reading
                result_keep_conn = True
            else:
                #no data read => reading finished, close the connection
                try:
                    if self._data_read_bs:
                        #some data read earlier => process all data obtained through this socket
                        content_type, header_bs, body_bs = _process_response(
                            self._data_read_bs, self.url_parts, self._info_msgs) #raises exceptions
                        if not self.url_parts.is_robot():
                            #check content type
                            if content_type not in CONTENT_TYPES:
                                self._info_msgs.append('content type %s rejected' % content_type)
                                raise util.CheckFailException(
                                    'Content type %s rejected' % content_type)
                            #check document size
                            if not body_bs or len(body_bs) < config.DOC_SIZE_MIN:
                                self._info_msgs.append('document too small')
                                raise util.CheckFailException(
                                    'Document too small: %d' % len(body_bs))
                    else:
                        #no data read ever
                        self._info_msgs.append('no data read')
                finally:
                    self.close()
                    result_keep_conn = False
        if result_keep_conn:
            if not self._socket_ready_time or \
                    util.seconds_left(self._socket_ready_time, CONN_RW_READY_PERIOD) <= 0:
                self._socket_try_count += 1
                self._socket_ready_time = util.now()
            self._last_action_time = util.now()
        return result_keep_conn, header_bs, body_bs

    def close(self):
        try:
            self._socket.close()
        except OSError as e:
            logging.debug('OSError %s in socket close ignored: %s' % (e.errno, e.strerror))
            pass #it is necessary to remove dead open connections
        logging.debug('CLOSE %s %s %s %s (%d B, %s)' %
            (self.url_parts.ip, self.url_parts.scheme, self.url_parts.host,
            self.url_parts.path[:MAX_PATH_LEN], len(self._data_read_bs),
            '; '.join(self._info_msgs)[-100:] or '-'))

    def check_last_action(self):
        fail = config.NO_ACTIVITY_TIMEOUT < util.seconds_total(util.now() - self._last_action_time)
        if fail:
            self.close()
        return not fail

    def get_last_action_time(self):
        return self._last_action_time

header_separator_bs_re = re.compile(b'(?s)\r?\n\r?\n')
header_line_separator_re = re.compile('\r?\n')
header_key_val_separator_re = re.compile('\s*\:\s*')
content_type_other_parts_re = re.compile('[;\s].*')
def _process_response(http_response_bs, url_parts, info_msgs):
    #split the HTTP response into header and body
    try:
        header_bs, body_bs = header_separator_bs_re.split(http_response_bs, maxsplit=1)
        body_bs = body_bs.strip()
    except ValueError:
        header_bs, body_bs = (http_response_bs, b'')
    try:
        header_lines = header_line_separator_re.split(header_bs.decode('utf-8', errors='replace'))
        response_line = header_lines.pop(0).strip()
        header_dict = {'_response': response_line}
        #process the 1st line of the HTTP response header
        if not response_line:
            raise IncorrectResponseError('Empty HTTP response first line')
        try:
            header_dict['_protocol'], code, header_dict['_msg'] = space_re.split(
                response_line, maxsplit=2)
            header_dict['_code'] = int(code)
        except ValueError:
            header_dict['_protocol'] = 'HTTP'
            try:
                code, header_dict['_msg'] = space_re.split(response_line, maxsplit=1)
                header_dict['_code'] = int(code)
            except ValueError:
                header_dict['_msg'] = ''
                try:
                    header_dict['_code'] = int(response_line)
                except ValueError:
                    raise IncorrectResponseError('Could not interpret HTTP response first line')
        #process the rest of the HTTP response header
        for header_line in header_lines:
            try:
                key, val = header_key_val_separator_re.split(header_line, maxsplit=1)
            except ValueError:
                continue
            header_dict[key.lower()] = val
    except Exception as e:
        info_msgs.append('incorrect response: %s' % e)
        raise IncorrectResponseError(str(e))
    info_msgs.append('HTTP %d' % header_dict['_code'])
    #HTTP features
    #redirection
    if header_dict['_code'] in HTTP_REDIRECT_CODES:
        try:
            redirect_url = header_dict['location']
        except UnicodeError:
            info_msgs.append('incorrect response: cannot decode redirect URL')
            raise IncorrectResponseError(
                'Cannot transform a redirect URL to unicode from utf-8')
        except KeyError:
            info_msgs.append('incorrect response: no redirect location')
            raise IncorrectResponseError(
                'Missing redirect location', remote_fault=True)
        try:
            redirect_url = url_join_rel_norm(url_parts.url, redirect_url)
        except ValueError:
            info_msgs.append('incorrect response: redirect URL urlparse failed')
            logging.debug('http.process_response URL join fail: %s -> %s' %
                (url_parts.url, redirect_url))
            raise IncorrectResponseError(
                'Incorrect URL (urlparse fails)', remote_fault=True)
        if redirect_url == url_parts.url:
            raise IncorrectResponseError('Redirect to the same URL %s' % redirect_url)
        raise RedirectException(redirect_url)
    #response not "200 OK"
    if 200 != header_dict['_code']:
        raise IncorrectResponseError(
            'Response code is %s, not 200' % header_dict['_code'], remote_fault=True)
    #chunked transfer encoding
    if 'chunked' == header_dict.get('transfer-encoding'):
        chunked_parts_bs = []
        chunk_start = 0
        while True:
            chunk_size_end = body_bs.find(b'\r\n', chunk_start)
            if chunk_size_end < 0 or chunk_size_end > chunk_start + 20:
                break #end or invalid => stop reading
            try:
                chunk_length = int(body_bs[chunk_start:chunk_size_end], 16)
            except ValueError:
                raise IncorrectDataChunkError()
            if chunk_length < 1 or chunk_length > 100000000:
                break #end or too long => stop reading
            chunk_data_start = chunk_size_end + 2
            chunk_data_end = chunk_data_start + chunk_length
            chunked_parts_bs.append(body_bs[chunk_data_start:chunk_data_end])
            chunk_start = chunk_data_end + 2
        body_bs = b''.join(chunked_parts_bs)
    #check the file (supposed to be robots.txt) is not a html file
    if body_bs and url_parts.is_robot():
        body_bs_sample_lower = body_bs[:200].lower()
        if b'<html' in body_bs_sample_lower or b'<body' in body_bs_sample_lower:
            info_msgs.append('robots.txt replaced by an HTML document')
            raise IncorrectResponseError(
                'robots.txt replaced by an HTML document', remote_fault=True)
    #return content type, header, body
    try:
        content_type = content_type_other_parts_re.sub('', header_dict['content-type']).strip()
    except KeyError:
        content_type = None
    return content_type, header_bs, body_bs
