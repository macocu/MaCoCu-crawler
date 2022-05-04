#!/usr/bin/env pypy3

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

"""
Downloader of data from the web
Two URL queues: an immediate download queue in RAM (q_urls) and a wait queue
in file for keeping URLs that cannot be downloaded soon (because the immediate
queue is full or in the case of non-robot over-represented host URLs).
Downloads data from the supplied URLs using multiple asynchronous connections. I/O:
- read paths to files with URLs from the standard input (from the scheduler)
- read the URLs to download from files
- write robots.txt to files
- write paths to files with robots.txt to the standard output (to the scheduler)
- write redirected URLs to files
- write paths to files with redirected URLs to the standard output
- write downloaded data to files
- write paths to files with downloaded data to a pipe output (to the processor)
"""

from errno import EINTR, ENOENT
import sys, io, shutil
import select
from time import sleep
from threading import Lock

import util
from util import http
from util import config

import logging
log_stream = io.open('%s/crawl.log' % config.LOG_DIR, mode='at',
    encoding='utf-8', errors='ignore', buffering=config.LOG_BUFFERING)
logging.basicConfig(stream=log_stream, level=config.LOG_LEVEL, format=config.LOG_FORMAT)

#initialise
scheduler_read_stream = io.open(sys.stdin.fileno(), mode='rt', encoding='utf-8',
    buffering=util.BUFSIZE, newline='\n')
scheduler_write_stream = io.open(sys.stdout.fileno(), mode='wt', encoding='utf-8',
    buffering=1, newline='\n')
epoll = select.epoll() #connections poller
conns_read = util.SafeDict() #open connections to read from
conns_write = util.SafeDict() #open connections to write to
q_urls = util.SafeUrlPartsDeque() #URL tuples to download:
    #<url, scheme, host, path, ip, src_scheme, src_host, redir_count>
dead_hosts = util.SafeSet() #dead hosts to be removed from waiting URLs
q_robots = util.SafeSet() #robots.txt data waiting to be sent
q_redirect = util.SafeSet() #redirect urls waiting to be sent
robot_redir = util.SafeSet() #robot redirection source and target host pairs
q_web_pages = util.SafeDeque() #raw web pages to store
q_web_page_files = dict((processor_id, util.SafeDeque()) for processor_id
    in range(config.DOC_PROCESSOR_COUNT)) #web page files to assign to document processors
q_urls_lock, q_urls_file_lock = Lock(), Lock()
urls_waiting_file_path = '%s/urls_waiting' % config.URL_DIR
robot_bytes = [0] #robots.txt bytes downloaded
wpage_bytes = [0] #web page bytes downloaded
terminate = [False] #termination flag
if util.THREAD_SWITCH_INTERVAL:
    sys.setswitchinterval(util.THREAD_SWITCH_INTERVAL)
logging.info('Downloader started')

#add wpage paths from a file to processors' queues (restarted crawling)
for arg in sys.argv[1:]:
    if arg.startswith('--wpage-paths-file='):
        wpage_paths_file_path = arg.split('=', 1)[1]
        processor_id = wpage_path_count = 0
        with io.open(wpage_paths_file_path, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
                newline='\n') as wpage_paths_file:
            for line in wpage_paths_file:
                q_web_page_files[processor_id].append_(line.strip())
                wpage_path_count += 1
                processor_id += 1
                if processor_id >= config.DOC_PROCESSOR_COUNT:
                    processor_id = 0
        logging.info('%d wpage file paths will be sent to document processors' % wpage_path_count)
        break

def read_from_scheduler():
    while True:
        msg = scheduler_read_stream.readline() #blocks until read
        #detect a termination message
        if msg.startswith(util.MSG_TERMINATE):
            logging.info('The downloader will be terminated')
            terminate[0] = True
            break
        #receive a file path with a list of URLs to download and domains/hosts to remove
        url_path = msg.rstrip()
        if not url_path:
            continue
        logging.debug('URL list to download: %s' % url_path)
        with io.open(url_path, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
                newline='\n') as url_file:
            url_data = url_file.read()
        with io.open('%s/urls_received' % config.URL_DIR, mode='at', encoding='utf-8',
                buffering=util.BUFSIZE) as urls_received_file:
            urls_received_file.write(url_data)
        util.remove_file_if_exists(url_path)
        #process the file, read URLs and dead domains
        #add new URLs to the queue (prevent a full queue or over-represented hosts)
        url_add_count, urls_waiting, new_dead_hosts = 0, [], set()
        q_urls_full = q_urls.len_() > config.MAX_URL_QUEUE
        with q_urls_lock:
            for url_line in url_data.split('\n'):
                if url_line.startswith(util.MSG_DEAD_DOMAIN):
                    dead_scheme, dead_host = url_line.split(' ')[1:]
                    new_dead_hosts.add((dead_host, dead_scheme))
                elif url_line:
                    try:
                        scheme, host, path, ip, robot_flag = url_line.split(' ')
                    except ValueError:
                        logging.warning('Ignoring incorrect URL "%s"' % url_line)
                        continue
                    url = http.url_join(scheme, host, path)
                    if robot_flag == util.ROBOT_FLAG:
                        q_urls.append_(http.UrlParts(url, scheme, host, path, ip, scheme, host, 0))
                        url_add_count += 1
                    else:
                        host_scheme = (host, scheme)
                        if q_urls_full \
                                or q_urls.host_count_(host_scheme) > config.MAX_HOST_URL_QUEUE:
                            #full queue or over-represented host => wait queue
                            urls_waiting.append(' '.join((url, scheme, host, path, ip)))
                        else:
                            q_urls.append_(http.UrlParts(url, scheme, host, path, ip))
                            url_add_count += 1
        #remove dead hosts
        if new_dead_hosts:
            dead_hosts.update_(new_dead_hosts)
            with q_urls_lock:
                q_urls.remove_hosts_(new_dead_hosts)
            logging.debug('%d dead hosts removed' % len(new_dead_hosts))
        #write non-robot over-represented host URLs to a file to wait
        wait_reason = ''
        if urls_waiting:
            with q_urls_file_lock:
                with io.open(urls_waiting_file_path, mode='at', encoding='utf-8',
                        buffering=util.BUFSIZE) as urls_waiting_file:
                    urls_waiting_file.write('%s\n' % '\n'.join(urls_waiting))
            wait_reason = ' (full queue)' if q_urls_full else ' (frequent host)'
        logging.debug('new URLs: %d added, %d waiting%s' %
            (url_add_count, len(urls_waiting), wait_reason))

def update_url_queue():
    #update the in memory URL queue by URLs waiting in a file
    #use only the top chunk of the file, rotate still waiting URLs to the bottom
    urls_waiting_file_path_tmp = '%s~' % urls_waiting_file_path
    period_start = util.now()
    while not terminate[0]:
        if util.seconds_left(period_start, config.UPDATE_WAITING_QUEUE_PERIOD) < 1:
            period_start = util.now()
            url_add_count = dead_host_count = 0
            urls_waiting, urls_wait_again = [], []
            with q_urls_file_lock:
                #use the first URL_WAITING_CHUNK_SIZE bytes from the file
                try:
                    with io.open(urls_waiting_file_path, mode='rt', encoding='utf-8',
                            buffering=util.BUFSIZE, newline='\n') as urls_waiting_file, \
                            io.open(urls_waiting_file_path_tmp, mode='wt', encoding='utf-8',
                            buffering=util.BUFSIZE) as urls_waiting_file_tmp:
                        url_data = urls_waiting_file.read(config.URL_WAITING_CHUNK_SIZE)
                        url_lines = url_data.split('\n')
                        urls_waiting_file_tmp.write('\n'.join(url_lines[-2:]))
                        while url_data:
                            url_data = urls_waiting_file.read(util.BUFSIZE)
                            urls_waiting_file_tmp.write(url_data)
                        del(url_data)
                        urls_waiting = [tuple(x.split(' ')) for x in url_lines[:-2]]
                        del(url_lines)
                    shutil.move(urls_waiting_file_path_tmp, urls_waiting_file_path)
                except IOError as e:
                    if e.errno == ENOENT:
                        continue #no waiting URLs yet
                #add waiting URLs to the queue (prevent a full queue or over-represented hosts)
                tmp_dead_hosts = dead_hosts.get_set_(clear=True)
                q_urls_full = q_urls.len_() > config.MAX_URL_QUEUE_UPDATED
                with q_urls_lock:
                    for url_parts in urls_waiting:
                        host_scheme = (url_parts[2], url_parts[1])
                        if host_scheme in tmp_dead_hosts:
                            dead_host_count += 1 #dead host URLs are not kept
                        elif q_urls_full \
                                or q_urls.host_count_(host_scheme) > config.MAX_HOST_URL_QUEUE:
                            urls_wait_again.append(' '.join(url_parts))
                        else:
                            q_urls.append_(http.UrlParts(*url_parts))
                            url_add_count += 1
                del(urls_waiting)
                #write URLs back to the file to wait again
                if urls_wait_again:
                    with io.open(urls_waiting_file_path, mode='at', encoding='utf-8',
                            buffering=util.BUFSIZE) as urls_waiting_file:
                        urls_waiting_file.write('%s\n' % '\n'.join(urls_wait_again))
                    wait_reason = ' (full queue)' if q_urls_full else ' (frequent host)'
                else:
                    util.remove_file_if_exists(urls_waiting_file_path)
                    wait_reason = ''
            logging.debug('update waiting URLs: %d added, %d dead host, %d waiting%s' %
                (url_add_count, dead_host_count, len(urls_wait_again), wait_reason))
            del(urls_wait_again)
        sleep(30)

def create_connections():
    LAST_CONN_TIME_CLEANUP_PERIOD = 300
    MAX_LAST_CONN_TIME = 60
    host_last_conn_time = {}
    ip_last_conn_time = {}
    last_conn_time_cleanup_time = util.now()
    while not terminate[0]:
        #full queue or max connections => wait
        if q_web_pages.len_() > config.MAX_WPAGE_QUEUE:
            logging.debug('full wpage queue delay')
            sleep(10)
            continue
        if conns_write.len_() + conns_read.len_() + config.OPEN_AT_ONCE > config.MAX_OPEN_CONNS:
            logging.debug('full conn queue delay')
            sleep(1)
            continue
        #get urls allowed to connect to immediately
        conns_skipped_host, conns_skipped_ip, selected_count = 0, 0, 0
        urls_now, hosts_now, ips_now, requeue, priority_requeue = [], set(), set(), [], []
        now_time = util.now()
        with q_urls_lock:
            while selected_count < config.OPEN_AT_ONCE:
                #pop a new url from the queue
                try:
                    url_parts = q_urls.popleft_()
                except IndexError:
                    break #empty url queue
                #host connection interval check (prevent connecting too early)
                host = url_parts.host
                try:
                    host_conn_period = util.seconds_total(now_time - host_last_conn_time[host])
                except KeyError:
                    host_last_conn_time[host] = now_time #new host
                else:
                    if host_conn_period < config.HOST_CONN_INTERVAL or host in hosts_now:
                        priority_requeue.append(url_parts) if url_parts.is_robot() \
                            else requeue.append(url_parts)
                        conns_skipped_host += 1
                        continue
                #IP connection interval check (prevent connecting too early)
                ip = url_parts.ip
                try:
                    ip_conn_period = util.seconds_total(now_time - ip_last_conn_time[ip])
                except KeyError:
                    ip_last_conn_time[ip] = now_time #new IP
                else:
                    if ip_conn_period < config.IP_CONN_INTERVAL or ip in ips_now:
                        priority_requeue.append(url_parts) if url_parts.is_robot() \
                            else requeue.append(url_parts)
                        conns_skipped_ip += 1
                        continue
                urls_now.append(url_parts)
                hosts_now.add(host)
                ips_now.add(ip)
                selected_count += 1
            q_urls.extendleft_(priority_requeue)
            q_urls.extend_(requeue)
        logging.debug('Conns selected: %d, waiting: %d/%d, Q: %d' %
            (selected_count, conns_skipped_host, conns_skipped_ip, q_urls.len_()))
        #open connections
        opened_count = 0
        now_time = util.now()
        for url_parts in urls_now:
            host_last_conn_time[url_parts.host] = now_time
            ip_last_conn_time[url_parts.ip] = now_time
            try:
                conn = http.Connection(url_parts)
            except http.ConnectionException:
                if url_parts.is_robot():
                    q_robots.add_((url_parts.scheme, url_parts.host, util.MSG_ROBOT_FAIL))
                continue
            try:
                conn_id = conn.connect(now_time)
            except http.ConnectionException:
                if url_parts.is_robot():
                    q_robots.add_((url_parts.scheme, url_parts.host, util.MSG_ROBOT_FAIL))
                continue
            #register the connection and add it to the write queue
            try:
                epoll.register(conn_id, select.EPOLLOUT)
            except IOError as e:
                conn.close()
                logging.warning('Unable to epoll.register a connection: %s' % e)
                continue
            conns_write.set_(conn_id, conn)
            opened_count += 1
        logging.debug('Conns opened: %d' % opened_count)
        #remove last connection time records for idle ip/hosts
        if util.seconds_left(last_conn_time_cleanup_time, LAST_CONN_TIME_CLEANUP_PERIOD) < 1:
            host_last_conn_time_new = {}
            for host, last_conn_time in host_last_conn_time.items():
                if util.seconds_left(last_conn_time, MAX_LAST_CONN_TIME) > 0:
                    host_last_conn_time_new[host] = last_conn_time
            host_last_conn_time = host_last_conn_time_new
            ip_last_conn_time_new = {}
            for ip, last_conn_time in host_last_conn_time.items():
                if util.seconds_left(last_conn_time, MAX_LAST_CONN_TIME) > 0:
                    ip_last_conn_time_new[ip] = last_conn_time
            ip_last_conn_time = ip_last_conn_time_new
            last_conn_time_cleanup_time = util.now()
        #wait if not at full load:
        if selected_count < config.OPEN_AT_ONCE:
            sleep(3)

def poll_connections():
    EPOLL_MAX_EVENTS = config.MAX_OPEN_CONNS + 1000
    MAX_ROBOT_REDIRECTS = 2 #max HTTP redirections of robots.txt
    while not terminate[0]:
        #poll connections, send & receive data
        try:
            events = epoll.poll(timeout=0.2, maxevents=EPOLL_MAX_EVENTS)
        except IOError as e:
            if e.errno == EINTR:
                logging.warning('Ignoring interrupted system call while epoll.poll')
                sleep(3)
            else:
                raise
        event_count = len(events)
        read_count = write_count = errhup_count = 0
        for conn_id, event in events:
            #write to connections ready for writing
            if event & select.EPOLLOUT:
                try:
                    conn = conns_write.get_strict_(conn_id)
                except KeyError:
                    continue
                write_count += 1
                #call write handle
                try:
                    write_result = conn.handle_write()
                except http.ConnectionException:
                    url_parts = conn.url_parts
                    if url_parts.is_robot():
                        q_robots.add_((url_parts.src_scheme or url_parts.scheme,
                            url_parts.src_host or url_parts.host, util.MSG_ROBOT_FAIL))
                    try:
                        conns_write.del_(conn_id)
                        epoll.unregister(conn_id)
                    except (KeyError, IOError):
                        pass #never mind
                else:
                    #switch the connection to reading
                    if write_result:
                        try:
                            conns_write.del_(conn_id)
                        except KeyError:
                            pass
                        try:
                            epoll.modify(conn_id, select.EPOLLIN)
                        except IOError:
                            try:
                                epoll.unregister(conn_id)
                            except IOError:
                                pass
                        else:
                            conns_read.set_(conn_id, conn)
            #read from connections ready for reading
            elif event & (select.EPOLLIN | select.EPOLLPRI):
                try:
                    conn = conns_read.get_strict_(conn_id)
                except KeyError:
                    continue
                read_count += 1
                url_parts = conn.url_parts
                is_robot = url_parts.is_robot()
                if is_robot:
                    scheme = url_parts.src_scheme or url_parts.scheme
                    host = url_parts.src_host or url_parts.host
                else:
                    scheme = url_parts.scheme
                    host = url_parts.host
                #call read handle
                keep_conn, header_bs, body_bs = False, b'', b''
                try:
                    keep_conn, header_bs, body_bs = conn.handle_read()
                except http.RedirectException as e:
                    #handle a redirection
                    redir_url = e.get_redirect_url()
                    if is_robot:
                        #redirected robots.txt => resolve here
                        redir_url_parts, bad_reason = http.url_split_and_check_parts(redir_url)
                        if bad_reason:
                            q_robots.add_((scheme, host, util.MSG_ROBOT_FAIL))
                            logging.debug('Failed robots redirection at %s://%s (%s)' %
                                (scheme, host, bad_reason))
                        else:
                            #add to q_urls with high priority
                            redir_scheme, redir_host, redir_path = redir_url_parts
                            new_redir_count = url_parts.redir_count + 1
                            if new_redir_count > MAX_ROBOT_REDIRECTS:
                                q_robots.add_((scheme, host, util.MSG_ROBOT_FAIL))
                                logging.debug('Failed robots redirection at %s://%s (%s)' %
                                    (scheme, host, 'redirected %d times' % new_redir_count))
                            else:
                                q_urls.appendleft_(http.UrlParts(redir_url, redir_scheme,
                                    redir_host, redir_path, ip=url_parts.ip, src_scheme=scheme,
                                    src_host=host, redir_count=new_redir_count))
                            #notify the scheduler about a redirected URL to a possibly new host
                            q_redirect.add_((scheme, host, '%s://%s/' % (redir_scheme, redir_host)))
                    else:
                        #redirected web page => resolve in the scheduler
                        q_redirect.add_((scheme, host, redir_url))
                except http.IncorrectResponseError as e:
                    if is_robot:
                        if e.is_remote_fault():
                            #not our fault
                            q_robots.add_((scheme, host, util.MSG_ROBOT_EMPTY))
                            logging.warning('Ignoring robots at %s %s (%s)' % (scheme, host, e))
                        else:
                            q_robots.add_((scheme, host, util.MSG_ROBOT_FAIL))
                except (http.HTTPException, http.ConnectionException, util.CheckFailException):
                    if is_robot:
                        q_robots.add_((scheme, host, util.MSG_ROBOT_FAIL))
                #remove a closed connection from the read queue
                if not keep_conn:
                    try:
                        conns_read.del_(conn_id)
                    except KeyError:
                        pass
                    try:
                        epoll.unregister(conn_id)
                    except IOError:
                        pass
                #put the result web page into web pages queue
                if is_robot:
                    if body_bs:
                        q_robots.add_((scheme, host, body_bs))
                    else:
                        q_robots.add_((scheme, host, util.MSG_ROBOT_EMPTY))
                elif body_bs:
                    q_web_pages.append_((conn, header_bs, body_bs))
            #remove erroneous/hanged up/other connections
            else:
                try:
                    conn = conns_read.pop_(conn_id)
                except KeyError:
                    try:
                        conn = conns_write.pop_(conn_id)
                    except KeyError:
                        conn = None
                if conn:
                    url_parts = conn.url_parts
                    if url_parts.is_robot():
                        q_robots.add_((url_parts.scheme, url_parts.host, util.MSG_ROBOT_FAIL))
                    errhup_count += 1
                try:
                    epoll.unregister(conn_id)
                except IOError:
                    pass
        logging.debug('Written to %d, read from %d, err %d connections' %
            (write_count, read_count, errhup_count))
        if event_count < 1:
            sleep(5)
        elif event_count < 10:
            sleep(3)
        elif event_count < 100:
            sleep(1)

def close_connections():
    #close inactive connections
    while not terminate[0]:
        closed_count = 0
        for conn_list in (conns_write, conns_read):
            for conn_id, conn in conn_list.items_():
                if not conn.check_last_action():
                    try:
                        conn_list.del_(conn_id)
                    except KeyError:
                        pass
                    try:
                        epoll.unregister(conn_id)
                    except IOError:
                        pass
                    if conn.url_parts.is_robot():
                        scheme, host = conn.url_parts.scheme, conn.url_parts.host
                        #no response after a long waiting => not our fault
                        q_robots.add_((scheme, host, util.MSG_ROBOT_EMPTY))
                        logging.warning('Ignoring robots at %s://%s (inactive connection)' %
                            (scheme, host))
                    closed_count += 1
        if closed_count:
            logging.debug('%d inactive connections closed' % closed_count)
        sleep(30)

def write_to_scheduler():
    robot_file_id = 0
    write_to_scheduler_delay = 30
    write_to_scheduler_delays = [config.MAX_WRITE_TO_SCHEDULER_DELAY, 50, 40, 30, 20] #cold start
    while not terminate[0]:
        #resolve robot redirections
        tmp_q_robots = q_robots.get_set_(clear=True)
        tmp_q_robots_d = {x[:2]: x[2] for x in tmp_q_robots}
        tmp_robot_redir = robot_redir.get_set_(clear=True)
        for scheme, host, redir_scheme, redir_host in tmp_robot_redir:
            try:
                robot_body_bs = tmp_q_robots_d[(redir_scheme, redir_host)]
            except KeyError:
                #target host robots not fetched yet => resolve the redirection later
                robot_redir.add_((scheme, host, redir_scheme, redir_host))
            else:
                #target host's robots fetched => add the data to the original host too
                tmp_q_robots.add((scheme, host, robot_body_bs))
        #send robot files to the scheduler
        robot_msgs = []
        for scheme, host, robot_data in tmp_q_robots:
            if robot_data in (util.MSG_ROBOT_EMPTY, util.MSG_ROBOT_FAIL):
                robot_msgs.append('%s %s %s\n' % (robot_data, scheme, host)) #decoded
            else:
                robot_body_bs = robot_data
                #decode the robots file
                try:
                    robot_body = robot_body_bs.decode('utf-8')
                except UnicodeError:
                    try:
                        robot_body = robot_body_bs.decode('iso-8859-1')
                    except UnicodeError:
                        robot_body = robot_body_bs.decode('utf-8', errors='replace')
                robot_bytes[0] += len(robot_body_bs)
                #save the robots file
                robot_path = '%s/%d' % (config.ROBOTS_DIR, robot_file_id)
                with io.open(robot_path, mode='wt', encoding='utf-8', buffering=util.BUFSIZE) \
                        as robot_file:
                    robot_file.write(robot_body)
                    robot_file.write('\n#%s://%s\n' % (scheme, host)) #add the host as a comment
                robot_msgs.append('%s %s %s %s\n' %
                    (util.MSG_ROBOT_FILE, scheme, host, robot_path))
                robot_file_id += 1
        if robot_msgs:
            scheduler_write_stream.write(''.join(robot_msgs)) #line buffered
            logging.debug('%d robot messages' % len(robot_msgs))
        sleep(write_to_scheduler_delay)
        if write_to_scheduler_delays:
            write_to_scheduler_delay = write_to_scheduler_delays.pop()
    logging.debug('%d robots written' % robot_file_id)

def write_web_pages():
    #write raw encoded web pages to files
    wpage_id = wpage_batch_id = processor_id = 0
    wpage_batch, wpage_batch_len = [], 0
    wpage_batch_size = 1
    wpage_batch_sizes = [config.MAX_WPAGE_BATCH_SIZE,
        200, 100, 50, 20, 10, 10, 5, 5, 2, 2, 1, 1, 1, 1, 1] #cold start
    while True:
        #get the connection data and the web page content
        try:
            conn, header_bs, body_bs = q_web_pages.popleft_()
        except IndexError:
            if not terminate[0]:
                sleep(10) #empty wpage queue
                continue
        else:
            header_len, body_len = len(header_bs), len(body_bs)
            wpage_bytes[0] += (header_len + body_len)
            wpage_id += 1
            url_parts = conn.url_parts
            conn_time = conn.get_last_action_time()
            wpage_header = '%d %s %s %s %s %s %s %d %d' % (
                wpage_id, url_parts.url, url_parts.scheme, url_parts.host, url_parts.path,
                url_parts.ip, conn_time.strftime('%Y%m%d%H%M%S'), header_len, body_len + 1)
            wpage_batch.append(b'%s\n%s%s\n' % (wpage_header.encode('utf-8'), header_bs, body_bs))
        #store the raw encoded web page data
        wpage_batch_len += 1
        if wpage_batch_len >= wpage_batch_size or terminate[0] and wpage_batch_len > 0:
            wpage_batch_id += 1
            wpage_path = '%s/%d' % (config.WPAGE_DIR, wpage_batch_id)
            with io.open(wpage_path, mode='wb', buffering=util.BUFSIZE) as web_page_file:
                web_page_file.write(b''.join(wpage_batch)) #encoded
            logging.debug('%d web pages written' % wpage_batch_len)
            wpage_batch, wpage_batch_len = [], 0
            q_web_page_files[processor_id].append_(wpage_path)
            processor_id += 1 #document processors take turns
            if processor_id >= config.DOC_PROCESSOR_COUNT:
                processor_id = 0
                if wpage_batch_sizes:
                    wpage_batch_size = wpage_batch_sizes.pop()
        if terminate[0]:
            break
    logging.debug('%d web pages in %d files written' % (wpage_id, wpage_batch_id))

def write_to_processor(processor_id, processor_pipe_path):
    redirects_written = wpage_paths_written = 0
    #assign a web page file to a document processor to process
    with io.open(processor_pipe_path, mode='wt', encoding='utf-8', buffering=1) \
            as processor_write_stream:
        while True:
            #redirections
            redirects = q_redirect.get_set_(clear=True)
            if redirects:
                for scheme, host, redir_url in redirects:
                    processor_write_stream.write('%s %s %s %s\n' %
                        (util.MSG_NEW_URL, scheme, host, redir_url)) #line buffered
                logging.debug('%d redirections written' % len(redirects))
                redirects_written += len(redirects)
            #web page data file paths
            wpage_paths = []
            while True:
                try:
                    wpage_paths.append(q_web_page_files[processor_id].popleft_())
                except IndexError:
                    break
            if wpage_paths:
                for wpage_path in wpage_paths:
                    processor_write_stream.write('%s\n' % wpage_path) #line buffered
                wpage_paths_written += len(wpage_paths)
            elif terminate[0]:
                break
            if len(redirects) + len(wpage_paths) < 1:
                sleep(20) #waiting for more redirections or web page files
    logging.debug('%d web page file paths written; %d redirections written' %
        (wpage_paths_written, redirects_written))

def print_info():
    total_bytes = wpage_bytes[0] + robot_bytes[0]
    q_web_page_files_len = sum(x.len_() for x in q_web_page_files.values())
    logging.info('URL Q: %d | MB got: %d | conns W: %d, conns R: %d | redir Q: %d, '
            'robot Q: %d, web page Q: %d, web page file Q: %d' %
        (q_urls.len_(), total_bytes // 1024**2, conns_write.len_(), conns_read.len_(),
        q_redirect.len_(), q_robots.len_(), q_web_pages.len_(), q_web_page_files_len))

def print_sizes():
    try:
        logging.info('SIZES [MB]: conns_read: %d, conns_write: %d, q_urls: %d, dead_hosts: %d, '
            'q_robots: %d, q_redirect: %d, robot_redir: %d, q_web_pages: %d, q_web_page_files: %d' %
            (util.get_size_mb(conns_read), util.get_size_mb(conns_write), util.get_size_mb(q_urls),
            util.get_size_mb(dead_hosts), util.get_size_mb(q_robots), util.get_size_mb(q_redirect),
            util.get_size_mb(robot_redir), util.get_size_mb(q_web_pages),
            util.get_size_mb(q_web_page_files)))
    except Exception as e:
        logging.info('SIZES: (could not determine sizes)')
        logging.debug('SIZES: Exception: %s' % e)

def print_info_loop():
    info_period_start = sizes_period_start = util.now()
    while not terminate[0]:
        if config.INFO_PERIOD and util.seconds_left(info_period_start, config.INFO_PERIOD) < 31:
            info_period_start = util.now()
            print_info()
        if config.SIZES_PERIOD and util.seconds_left(sizes_period_start, config.SIZES_PERIOD) < 31:
            sizes_period_start = util.now()
            print_sizes()
        sleep(60)

#obtain IDs of the external document processors
doc_processor_pipe_ids = []
for i in range(config.DOC_PROCESSOR_COUNT):
    processor_id, processor_pipe_path = scheduler_read_stream.readline().rstrip().split(' ')
    doc_processor_pipe_ids.append((int(processor_id), processor_pipe_path))
#thread receiving & processing new URLs
t_sched_read = util.LogThread(logging, target=read_from_scheduler, name='read_from_scheduler')
t_sched_read.start()
#thread updating the URL queue by waiting URLs
t_update_urls = util.LogThread(logging, target=update_url_queue, name='update_url_queue')
t_update_urls.start()
#thread creating new connections
t_conns = util.LogThread(logging, target=create_connections, name='create_connections')
t_conns.start()
#thread polling the connections
t_poll = util.LogThread(logging, target=poll_connections, name='poll_connections')
t_poll.start()
#thread closing inactive connections
t_alive = util.LogThread(logging, target=close_connections, name='close_connections')
t_alive.start()
#thread sending data to the scheduler
t_sched_write = util.LogThread(logging, target=write_to_scheduler, name='write_to_scheduler')
t_sched_write.start()
#thread storing downloaded raw web pages to files
t_wpage_write = util.LogThread(logging, target=write_web_pages, name='write_web_pages')
t_wpage_write.start()
#threads assigning stored web pages files to processors
t_proc_write = {}
for processor_id, processor_pipe_path in doc_processor_pipe_ids:
    t_proc_write[processor_id] = util.LogThread(logging, target=write_to_processor,
        name='write_to_processor %d' % processor_id, args=(processor_id, processor_pipe_path))
    t_proc_write[processor_id].start()
#thread printing debug information periodically
if config.INFO_PERIOD or config.SIZES_PERIOD:
    t_info = util.LogThread(logging, target=print_info_loop, name='print_info_loop')
    t_info.start()
#wait for threads to finish
t_sched_read.join()
if config.INFO_PERIOD:
    print_info()
if config.SIZES_PERIOD:
    print_sizes()
t_update_urls.join()
t_conns.join()
t_poll.join()
t_alive.join()
t_sched_write.join()
t_wpage_write.join()
for processor_id, processor_pipe_path in doc_processor_pipe_ids:
    t_proc_write[processor_id].join()
if config.INFO_PERIOD or config.SIZES_PERIOD:
    t_info.join()
epoll.close()
with q_urls_lock:
    url_tuples_l = (' '.join((u.url, u.scheme, u.host, u.path, str(u.ip)))
        for u in q_urls.iteritems_())
with io.open('%s/download_queue' % config.URL_DIR, mode='wt', encoding='utf-8',
        buffering=util.BUFSIZE) as url_parts_file:
    url_parts_file.write('\n'.join(url_tuples_l))
scheduler_write_stream.write('%s\n' % util.MSG_TERMINATE) #line buffered
logging.info('Downloader finished, %d MB downloaded' %
    ((wpage_bytes[0] + robot_bytes[0]) // 1024**2))
