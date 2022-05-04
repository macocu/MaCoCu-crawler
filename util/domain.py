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

import io, logging, re
from threading import Lock
from urllib.parse import quote as url_quote

from .robotparser import RobotExclusionRulesParser
import util
from util import config
from util.http import NO_HOST, ROBOT_PATH

MID_DOMAIN_NEW_PATHS = (config.MAX_DOMAIN_NEW_PATHS * 3 + config.MIN_DOMAIN_NEW_PATHS) // 4
RP_NONE, RP_INIT, RP_READY, RP_FAIL, RP_EMPTY = 0, 1, 2, 3, 4
PATH_BOILERPLATE = '[&;#/?](?:(?:(?:(?:j|php)s(?:ess|ession)?|auth)_?(?:id)?' \
    '|(?:s(?:ess|ession)?|auth)_?id)|utm_source)=.*'
PATH_BOILERPLATE_RE = re.compile(PATH_BOILERPLATE, re.IGNORECASE) #!TODO to util/http.py

def _host2fname(scheme_host):
    return url_quote('%s_%s' % scheme_host)[:250]

class Domain(object):
    __slots__ = ('_host', '_scheme', '_ip', '_bad', '_paths_hashes', '_paths_new',
        '_paths_file_empty', '_paths_new_lock', '_count_downloaded', '_count_cleaned',
        '_bytes_downloaded', '_bytes_cleaned', '_robot_parser', '_robot_state',
        '_last_action_time', '_creation_time')
    if config.MULTILINGUAL:
        __slots__ += ('_bytes_cleaned_primary_lang',)

    def __init__(self, scheme_host, ip=None, bad=False, paths_hashes={}, paths_new={},
            count_downloaded=0, count_cleaned=0, bytes_downloaded=0, bytes_cleaned=0,
            bytes_cleaned_primary_lang=0):
        self._scheme, self._host = scheme_host
        self._ip = ip
        self._bad = bad
        self._paths_hashes = util.SafeUniqueSet(paths_hashes) #all paths' hashes
        self._paths_new = util.SafeDeque(paths_new) #paths waiting to be sent for download
        self._paths_file_empty = True #paths put away to a file
        self._paths_new_lock = Lock()
        self._count_downloaded = count_downloaded #number of paths successfuly downloaded (html)
        self._count_cleaned = count_cleaned #number of paths successfuly cleaned (txt)
        self._bytes_downloaded = bytes_downloaded
        self._bytes_cleaned = bytes_cleaned
        if config.MULTILINGUAL:
            self._bytes_cleaned_primary_lang = bytes_cleaned_primary_lang
        self._robot_parser = SafeRobotExclusionRulesParser()
        self._robot_state = RP_NONE
        self._last_action_time = self._creation_time = util.now()

    def get_host(self):
        return self._host
    def get_scheme(self):
        return self._scheme
    def get_ip(self):
        return self._ip
    def is_bad(self):
        return self._bad
    def is_sleeping(self):
        return False

    def set_ip(self, ip):
        if ip in (NO_HOST, None):
            self._bad = True
            logging.debug('Bad IP at %s: %s' % (self._host, ip))
        else:
            self._ip = ip
            return True

    def is_robot_ready(self):
        if self._robot_state == RP_INIT and config.MAX_ROBOT_WAITING_TIME \
                and util.seconds_left(self._creation_time, config.MAX_ROBOT_WAITING_TIME) < 1:
            logging.debug('Waiting too long for robots at %s' % self._host)
            self.set_robots(util.MSG_ROBOT_FAIL)
        return self._robot_state in (RP_READY, RP_FAIL, RP_EMPTY) and not self.is_bad()

    def set_robots(self, robot_body, save_to_file=True):
        if not robot_body or robot_body.startswith(util.MSG_ROBOT_EMPTY):
            self._robot_state = RP_EMPTY
            robot_body = util.MSG_ROBOT_EMPTY
        elif robot_body.startswith(util.MSG_ROBOT_FAIL):
            self._robot_state = RP_FAIL
            robot_body = util.MSG_ROBOT_FAIL
        else:
            try:
                crawl_delay = self._robot_parser.parse_and_get_crawl_delay(robot_body)
            except Exception as e:
                logging.debug('Robot parser parse exception at %s: %s' % (self._host, e))
                self._robot_state = RP_FAIL
            else:
                self._robot_state = RP_READY
                #filter out paths not allowed for robots
                self._paths_new.filter_(lambda x: self.is_robot_allowed(x))
                if crawl_delay and crawl_delay > config.HOST_CONN_INTERVAL:
                    logging.warning('Crawl delay decreased from %d to %d at %s' %
                        (crawl_delay, config.HOST_CONN_INTERVAL, self._host))
        if self._robot_state == RP_FAIL:
            if config.IGNORE_ROBOTS_WHEN_FAILED:
                logging.warning('Ignoring robots (failed to fetch) at %s' % self._host)
            else:
                self._bad = True
        #save to file (failed => no file)
        if save_to_file and not self._bad and self._robot_state != RP_FAIL:
            with io.open('%s/%s' % (config.DOM_ROBOT_DIR, _host2fname((self._scheme, self._host))),
                    mode='wt', encoding='utf-8', buffering=util.BUFSIZE) as domain_robot_file:
                domain_robot_file.write(robot_body)

    def is_robot_allowed(self, path):
        if self._robot_state == RP_READY:
            try:
                return self._robot_parser.is_allowed(path)
            except Exception as e:
                logging.warning('Robot parser is_allowed exception at %s://%s%s: %s' %
                    (self._scheme, self._host, path, e))
                return config.IGNORE_ROBOTS_WHEN_FAILED
        if self._robot_state == RP_EMPTY or (self._robot_state == RP_FAIL and
                config.IGNORE_ROBOTS_WHEN_FAILED):
            return True
        return False

    def add_new_paths(self, paths):
        #add new paths
        path_list = []
        for path in paths:
            #check path has not been added yet (ignoring slashes in the path)
            #try to eliminate differences in session id too
            path_sanitised = PATH_BOILERPLATE_RE.sub('', path)
            try:
                self._paths_hashes.add_unique_(hash(path_sanitised))
            except util.ItemNotUniqueException:
                continue
            #check path is allowed for robots
            if self._robot_state in (RP_NONE, RP_INIT) or self.is_robot_allowed(path):
                path_list.append(path)
        self._paths_new.extend_(path_list)
        self._last_action_time = util.now()
        #optimise the order and count of new paths
        with self._paths_new_lock:
            #sort paths by length (then by append order) to download short paths first
            path_list = list(sorted(self._paths_new.get_list_(clear=True), key=len))
            #write some paths to a file if too many paths
            if len(path_list) > config.MAX_DOMAIN_NEW_PATHS:
                paths_file_path = '%s/%s' % (config.DOM_PATH_DIR,
                    _host2fname((self._scheme, self._host)))
                with io.open(paths_file_path, mode='at', encoding='utf-8',
                        buffering=util.BUFSIZE) as paths_file:
                    paths_file.write('%s\n' % '\n'.join(path_list[MID_DOMAIN_NEW_PATHS:]))
                self._paths_file_empty = False
                logging.debug('%d new paths saved to file at %s://%s' %
                    (len(path_list) - MID_DOMAIN_NEW_PATHS, self._scheme, self._host))
                path_list = path_list[:MID_DOMAIN_NEW_PATHS] #aim to 3/4 full
            self._paths_new.extend_(path_list)

    def get_url_tuples_to_download(self, max_count):
        #returns list of new path tuples: scheme, host, path, ip, robot_flag
        if self._bad:
            return []
        if RP_NONE == self._robot_state:
            self._robot_state = RP_INIT
            return [(self._scheme, self._host, ROBOT_PATH, self._ip, util.ROBOT_FLAG)]
        if self._robot_state not in (RP_EMPTY, RP_READY):
            return []
        result = []
        for _ in range(max_count):
            try:
                path = self._paths_new.popleft_()
            except IndexError:
                break
            result.append((self._scheme, self._host, path, self._ip, util.NOROBOT_FLAG))
        #read more new paths from a file in case too few new paths
        with self._paths_new_lock:
            paths_new_len = self._paths_new.len_()
            if not self._paths_file_empty and paths_new_len < config.MIN_DOMAIN_NEW_PATHS:
                paths_file_path = '%s/%s' % (config.DOM_PATH_DIR,
                    _host2fname((self._scheme, self._host)))
                with io.open(paths_file_path, mode='rt', encoding='utf-8',
                        buffering=util.BUFSIZE) as paths_file:
                    paths_saved = [x for x in paths_file.read().split('\n') if x]
                path_count_to_add = MID_DOMAIN_NEW_PATHS - paths_new_len
                path_list = self._paths_new.get_list_(clear=True) + paths_saved[:path_count_to_add]
                #sort paths by length (then by append order) to download short paths first
                self._paths_new.extend_(list(sorted(path_list, key=len)))
                #write the rest back or delete the file if no paths left
                paths_left = paths_saved[path_count_to_add:]
                if paths_left:
                    with io.open(paths_file_path, mode='wt', encoding='utf-8',
                            buffering=util.BUFSIZE) as paths_file:
                        paths_file.write('%s\n' % '\n'.join(paths_left))
                else:
                    util.remove_file_if_exists(paths_file_path)
                    self._paths_file_empty = True
                logging.debug('%d/%d new paths read/left from file at %s://%s' %
                    (len(path_list) - paths_new_len, len(paths_left), self._scheme, self._host))
        return result

    def get_new_path_count(self):
        return self._paths_new.len_()

    def add_size_downloaded(self, size):
        self._count_downloaded += 1
        self._bytes_downloaded += size
        self._last_action_time = util.now()

    def add_size_cleaned(self, size, is_primary_lang=None):
        self._count_cleaned += 1
        self._bytes_cleaned += size
        if config.MULTILINGUAL and is_primary_lang:
            self._bytes_cleaned_primary_lang += size

    def get_count_cleaned(self):
        return self._count_cleaned

    def is_efficient(self):
        #don't check the efficiency if not enough data downloaded
        if self._bytes_downloaded < config.MIN_BYTES_DOWNLOADED or \
                self._count_downloaded < config.MIN_DOCS_DOWNLOADED:
            return True
        #check the text yield ratio threshold is met
        text_yield_ratio_threshold = config.yield_rate_threshold(self._count_downloaded)
        if self._bytes_cleaned / self._bytes_downloaded < text_yield_ratio_threshold:
            self._bad = True
            return False
        #check the primary language ratio threshold is met
        if config.MULTILINGUAL:
            primary_lang_ratio_thr = config.primary_lang_ratio_threshold(self._count_downloaded)
            if self._bytes_cleaned_primary_lang / self._bytes_cleaned < primary_lang_ratio_thr:
                self._bad = True
                return False
        return True

    def is_idle(self):
        return (self._paths_file_empty and self._paths_new.len_() < 1 and
            util.seconds_left(self._last_action_time, config.MAX_IDLE_TIME) < 1)

    def update_last_action_time(self):
        self._last_action_time = util.now()

    @classmethod
    def load_from_bytes(cls, bs):
        p1_bs, p2p3_bs = bs.split(b'\n', 1) #encoded
        if config.MULTILINGUAL:
            scheme_host, ip, distance, is_bad, bytes_downloaded, bytes_cleaned, count_downloaded, \
                count_cleaned, bytes_cleaned_primary_lang, p2_len, p3_len \
                = p1_bs.decode('utf-8').split(' ')
        else:
            scheme_host, ip, distance, is_bad, bytes_downloaded, bytes_cleaned, count_downloaded, \
                count_cleaned, p2_len, p3_len = p1_bs.decode('utf-8').split(' ')
            bytes_cleaned_primary_lang = 0
        scheme, host = scheme_host.split('://', 1)
        p2_len, p3_len = int(p2_len), int(p3_len)
        paths_new, paths_hashes = set(), set()
        if p2_len:
            paths_new.update([x for x in p2p3_bs[:p2_len].decode('utf-8').split('\n') if x])
        if p3_len:
            paths_hashes.update(util.unpack_ints(p2p3_bs[-p3_len - 1:-1]))
        return cls((scheme, host), ip, is_bad == 'BAD', paths_hashes, paths_new,
            int(count_downloaded), int(count_cleaned), int(bytes_downloaded),
            int(bytes_cleaned), int(bytes_cleaned_primary_lang)), int(distance)

    def save_to_bytes(self, distance):
        p1 = '%s://%s %s %d %s %d %d %d %d' % (self._scheme, self._host, self._ip, distance,
            'BAD' if self._bad else 'OK', self._bytes_downloaded, self._bytes_cleaned,
            self._count_downloaded, self._count_cleaned)
        if config.MULTILINGUAL:
            p1 = '%s %d' % (p1, self._bytes_cleaned_primary_lang)
        p2_bs = '\n'.join(self._paths_new.get_list_()).encode('utf-8') #encoded
        p3_bs = util.pack_ints(self._paths_hashes.get_set_()) #encoded
        return b'%s %d %d\n%s\n%s\n' % \
            (p1.encode('utf-8'), len(p2_bs), len(p3_bs), p2_bs, p3_bs) #encoded

    @staticmethod
    def save_empty_domain_to_bytes(scheme_host, distance):
        result = '%s://%s None %d BAD 0 0 0 0 0 0\n\n\n' % (*scheme_host, distance)
        if config.MULTILINGUAL:
            result = result.replace('\n\n\n', ' 0\n\n\n')
        return result.encode('utf-8') #encoded

    @classmethod
    def load_from_file(cls, scheme_host, delete_file=False):
        domain_file_path = '%s/%s' % (config.DOM_SLEEP_DIR, _host2fname(scheme_host))
        with io.open(domain_file_path, mode='rb', buffering=util.BUFSIZE) as domain_file:
            domain, distance = cls.load_from_bytes(domain_file.read())
        if delete_file:
            util.remove_file_if_exists(domain_file_path)
        #load the robot file (saved when setting robots)
        if not domain.is_bad():
            try:
                with io.open('%s/%s' % (config.DOM_ROBOT_DIR, _host2fname(scheme_host)),
                        mode='rt', encoding='utf-8', buffering=util.BUFSIZE) as domain_robot_file:
                    robot_body = domain_robot_file.read()
            except IOError:
                robot_body = util.MSG_ROBOT_FAIL #no file => failed
            domain.set_robots(robot_body, save_to_file=False)
        return domain, distance

    def save_to_file(self, distance):
        with io.open('%s/%s' % (config.DOM_SLEEP_DIR, _host2fname((self._scheme, self._host))),
                mode='wb', buffering=util.BUFSIZE) as domain_file:
            domain_file.write(self.save_to_bytes(distance))
        #saving the robot file not needed (saved when setting robots)

class SafeRobotExclusionRulesParser(object):
    __slots__ = ('_robot_parser', '_lock')

    def __init__(self):
        self._robot_parser = RobotExclusionRulesParser()
        self._lock = Lock()

    def parse_and_get_crawl_delay(self, robot_rules):
        with self._lock:
            self._robot_parser.parse(robot_rules)
            try:
                return float(self._robot_parser.get_crawl_delay(config.AGENT))
            except (ValueError, TypeError):
                return

    def is_allowed(self, path_bs):
        with self._lock:
            return self._robot_parser.is_allowed(config.AGENT, path_bs)
