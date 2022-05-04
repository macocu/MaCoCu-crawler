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

import datetime, os, re, struct, subprocess, sys
from collections import deque
from errno import EEXIST, ENOENT
from threading import Lock, Thread
from traceback import format_exc

#== interprocess communication ==
MSG_TERMINATE = 'ORDER_66'
MSG_NEW_URL = 'NEW_URL'
MSG_DEAD_DOMAIN = 'DEAD_DOM'
MSG_ROBOT_FILE = 'ROBOT_FILE'
MSG_ROBOT_FAIL = 'ROBOT_FAIL'
MSG_ROBOT_EMPTY = 'ROBOT_EMPTY'
ROBOT_FLAG = 'ROBOT'
NOROBOT_FLAG = ''
BUFSIZE = 256*1024

#== threads ==
#interpreter's signal and thread switch check interval [s],
#set to None to keep the default (0.005 s in CPython)
THREAD_SWITCH_INTERVAL = None

"Thread safety by delegation. Only thread safe methods exposed."
class SafeCollection(object):
    __slots__ = ('_obj', '_lock')

    def __init__(self, *args, **kwargs):
        self._obj = None
        self._lock = Lock()

    def len_(self):
        with self._lock:
            return len(self._obj)

    def get_(self, k, default=None):
        with self._lock:
            return self._obj.get(k, default)

    def get_strict_(self, k):
        with self._lock:
            return self._obj[k]

    def in_(self, k):
        with self._lock:
            return k in self._obj

    def add_(self, x):
        with self._lock:
            self._obj.add(x)

    def pop_(self):
        with self._lock:
            return self._obj.pop()

    def popleft_(self):
        with self._lock:
            return self._obj.popleft()

    def popitem_(self):
        with self._lock:
            return self._obj.popitem()

    def update_(self, x):
        with self._lock:
            return self._obj.update(x)

    def set_(self, k, v):
        with self._lock:
            self._obj[k] = v

    def del_(self, k, strict=False):
        with self._lock:
            try:
                del(self._obj[k])
            except KeyError:
                if strict:
                    raise

    def remove_(self, k, strict=False):
        with self._lock:
            try:
                self._obj.remove(k)
            except ValueError:
                if strict:
                    raise

    def append_(self, x):
        with self._lock:
            self._obj.append(x)

    def appendleft_(self, x):
        with self._lock:
            self._obj.appendleft(x)

    def extend_(self, x):
        with self._lock:
            return self._obj.extend(x)

    def extendleft_(self, x):
        with self._lock:
            return self._obj.extendleft(x)

    def clear_(self):
        with self._lock:
            self._obj.clear()

    def items_(self):
        with self._lock:
            return set(self._obj.items())

    def keys_(self):
        with self._lock:
            return set(self._obj.keys())

    def values_(self):
        with self._lock:
            return set(self._obj.values())

class SafeSet(SafeCollection):
    def __init__(self, initial=(), *args, **kwargs):
        super(SafeSet, self).__init__(*args, **kwargs)
        self._obj = set(initial)

    def get_set_(self, clear=False):
        with self._lock:
            items = set(self._obj)
            if clear:
                self._obj.clear()
            return items

    def filter_(self, f):
        with self._lock:
            new_items = list(filter(f, self._obj))
            self._obj.clear()
            self._obj.update(new_items)

class ItemNotUniqueException(Exception):
    pass

class SafeUniqueSet(SafeSet):
    def add_unique_(self, x):
        with self._lock:
            if x in self._obj:
                raise ItemNotUniqueException()
            self._obj.add(x)

class SafeDict(SafeCollection):
    def __init__(self, initial={}, *args, **kwargs):
        super(SafeDict, self).__init__(*args, **kwargs)
        self._obj = dict(initial)

    def pop_(self, k, default=None):
        with self._lock:
            return self._obj.pop(k, default)

class SafeDeque(SafeCollection):
    def __init__(self, initial=(), *args, **kwargs):
        super(SafeDeque, self).__init__(*args, **kwargs)
        self._obj = deque(initial)

    def filter_(self, f):
        with self._lock:
            new_items = list(filter(f, self._obj))
            self._obj.clear()
            self._obj.extend(new_items)

    def items_(self):
        with self._lock:
            return list(self._obj)

    def get_list_(self, clear=False):
        with self._lock:
            items = list(self._obj)
            if clear:
                self._obj.clear()
            return items

class SafeUrlPartsDeque(object):
    __slots__ = ('_url_parts', '_host_count', '_lock')

    def __init__(self, initial=()):
        self._url_parts = deque(initial)
        self._host_count = {}
        self._lock = Lock()

    def len_(self):
        with self._lock:
            return len(self._url_parts)

    def host_count_(self, host_scheme):
        with self._lock:
            return self._host_count.get(host_scheme, 0)

    def append_(self, url_parts):
        with self._lock:
            host_scheme = (url_parts.host, url_parts.scheme)
            self._host_count[host_scheme] = self._host_count.get(host_scheme, 0) + 1
            self._url_parts.append(url_parts)

    def appendleft_(self, url_parts):
        with self._lock:
            host_scheme = (url_parts.host, url_parts.scheme)
            self._host_count[host_scheme] = self._host_count.get(host_scheme, 0) + 1
            self._url_parts.appendleft(url_parts)

    def extend_(self, url_parts_l):
        with self._lock:
            for url_parts in url_parts_l:
                host_scheme = (url_parts.host, url_parts.scheme)
                self._host_count[host_scheme] = self._host_count.get(host_scheme, 0) + 1
            return self._url_parts.extend(url_parts_l)

    def extendleft_(self, url_parts_l):
        with self._lock:
            for url_parts in url_parts_l:
                host_scheme = (url_parts.host, url_parts.scheme)
                self._host_count[host_scheme] = self._host_count.get(host_scheme, 0) + 1
            return self._url_parts.extendleft(url_parts_l)

    def popleft_(self):
        with self._lock:
            url_parts = self._url_parts.popleft()
            try:
                self._host_count[(url_parts.host, url_parts.scheme)] -= 1
            except KeyError:
                pass
            return url_parts

    def remove_hosts_(self, host_scheme_l):
        with self._lock:
            for host_scheme in host_scheme_l:
                try:
                    del(self._host_count[host_scheme])
                except KeyError:
                    pass
            filtered_url_parts = [x for x in self._url_parts if x not in host_scheme_l]
            self._url_parts.clear()
            self._url_parts.extend(filtered_url_parts)

    def iteritems_(self):
        with self._lock:
            for url_parts in self._url_parts:
                yield url_parts

class CheckFailException(Exception):
    pass

class LogThread(Thread):
    "Inspired by Ben Leslie, http://benno.id.au/blog/2012/10/06/python-thread-exceptions"
    def __init__(self, logger, **kwargs):
        super(LogThread, self).__init__(**kwargs)
        self._real_run = self.run
        self.run = self._wrap_run
        self._logger = logger

    def _wrap_run(self):
        try:
            self._real_run()
        except Exception as e:
            self._logger.error('Exception in thread %s: %s' % (self, e))
            self._logger.error(format_exc())
            raise
        self._logger.debug('Finished')

#== other ==
now = datetime.datetime.now

def seconds_total(timedelta_):
    return timedelta_.seconds + timedelta_.days * 24 * 3600

def seconds_left(time_start, duration):
    return max(0, duration - seconds_total(now() - time_start))

def pack_ints(ints):
    try:
        return struct.pack('l' * len(ints), *ints)
    except TypeError:
        return ''

def unpack_ints(packed_ints):
    return struct.unpack('l' * (len(packed_ints) // 8), packed_ints)

space_re = re.compile('\s+')
backslash_re = re.compile(r'\\')
def clean_xml_val(s):
    return backslash_re.sub('', space_re.sub(' ', s.strip().replace('"', '&quot;').replace(
        '<', '&lt;').replace('>', '&gt;').replace("'", '&apos;')))

def create_dir_if_not_exists(path):
    try:
        os.mkdir(path)
    except OSError as e:
        if e.errno != EEXIST:
            raise

def create_pipe_rewrite_if_exists(path):
    try:
        os.mkfifo(path)
    except OSError as e:
        if e.errno == EEXIST:
            os.unlink(path)
            os.mkfifo(path)
        else:
            raise

def remove_file_if_exists(path):
    try:
        os.unlink(path)
    except OSError as e:
        if e.errno != ENOENT:
            raise

def link_force(source, link_name, hard=True):
    link_f = os.link if hard else os.symlink
    try:
        link_f(source, link_name)
    except OSError as e:
        if e.errno != EEXIST:
            raise
        os.unlink(link_name)
        link_f(source, link_name)

def increasing_range(start, end, step_count=10, repeat_start_count=1):
    return [start] * repeat_start_count + list(range(start, end, end // step_count)) + [end]

JUSTEXT_PARAMS_BY_STRICTNESS_LEVEL = {
    'verystrict': { #Justext default
        'length_low': 70,
        'length_high': 200,
        'stopwords_low': 0.3,
        'stopwords_high': 0.32,
        'max_link_density': 0.2,
        'max_heading_distance': 200,
    },
    'strict': {
        'length_low': 70,
        'length_high': 200,
        'stopwords_low': 0.25,
        'stopwords_high': 0.32,
        'max_link_density': 0.3,
        'max_heading_distance': 250,
    },
    'balanced': {
        'length_low': 55,
        'length_high': 140,
        'stopwords_low': 0.2,
        'stopwords_high': 0.3,
        'max_link_density': 0.4,
        'max_heading_distance': 300,
    },
    'permissive': {
        'length_low': 40,
        'length_high': 90,
        'stopwords_low': 0.2,
        'stopwords_high': 0.3,
        'max_link_density': 0.45,
        'max_heading_distance': 500,
    },
}

#Recursively find size of objects in bytes, https://github.com/bosswissam/pysize, MIT licence
from inspect import isgetsetdescriptor, ismemberdescriptor
def get_size(obj, seen=None):
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id) #mark as seen *before* entering recursion to handle self-referential objects
    if hasattr(obj, '__dict__'):
        for cls in obj.__class__.__mro__:
            if '__dict__' in cls.__dict__:
                d = cls.__dict__['__dict__']
                if isgetsetdescriptor(d) or ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break
    if isinstance(obj, dict):
        size += sum((get_size(v, seen) for v in obj.values()))
        size += sum((get_size(k, seen) for k in obj.keys()))
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum((get_size(i, seen) for i in obj))
    if hasattr(obj, '__slots__'): # can have __slots__ with __dict__
        size += sum(get_size(getattr(obj, s), seen) for s in obj.__slots__ if hasattr(obj, s))
    return size
def get_size_mb(obj):
    return get_size(obj) // 1024**2
