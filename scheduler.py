#!/usr/bin/env python3

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
Crawling scheduler
Creates subprocesses: a downloader and multiple data processors. Directs traversing the web.
Keeps web domains with paths, selects the domains to crawl, selects URLs to download. I/O:
- read seed URLs from the standard input
- write a processor ID to processors' standard input
- read paths to files with robots.txt from the downloader's standard output
- read robots.txt from files
- read paths to files with URL redirections from the downloader's standard output
- read URL redirections from files
- read paths to files with processed documents and URLs from the processors' standard output
- read processed documents and extracted URLs from files
- write IDs of duplicate documents to files
- write URLs to download to files
- write paths to files with URLs to download to the downloader's standard input
- write domains to stop downloading to files
- write paths to files with domains to stop downloading to the downloader's standard input
"""

import sys, os, io, gzip
import signal
import subprocess
from time import sleep

import util
from util import config
from util.domain import Domain
from util.http import url_split, get_ip

import logging
util.create_dir_if_not_exists(config.RUN_DIR)
util.create_dir_if_not_exists(config.LOG_DIR)
log_stream = io.open('%s/schedule.log' % config.LOG_DIR, mode='at',
    encoding='utf-8', errors='ignore', buffering=config.LOG_BUFFERING)
logging.basicConfig(stream=log_stream, level=config.LOG_LEVEL, format=config.LOG_FORMAT)

#initialise
USAGE = """
A web TLD spider/crawler. See README.
Usage: pypy3|python3 %s [--state-files=DOMAINS,RAW_HASHES,TXT_HASHES]
    [--seed-triples|--old-tuples] < SEED_URLS
SEED_URLS is a UTF-8 text file with one URL per line (e.g. `https://www.sketchengine.eu/')
    or one space separated triple <scheme, host, path> per line if --seed-triples
    or the same to load to the downloader directly (e.g. with urls_waiting) if --old-tuples
    (e.g. `https www.sketchengine.eu /')
""" % sys.argv[0]
if '-h' in sys.argv or '--help' in sys.argv:
    sys.stderr.write(USAGE)
    sys.exit(1)
run_dirs = (config.RUN_DIR, config.LOG_DIR, config.PIPE_DIR, config.URL_DIR, config.ROBOTS_DIR,
    config.REDIRECT_DIR, config.WPAGE_DIR, config.ARC_DIR, config.DOC_META_DIR, config.PREVERT_DIR,
    config.IGNORED_DIR, config.DOM_SLEEP_DIR, config.DOM_PATH_DIR, config.DOM_ROBOT_DIR,
    config.SAVE_DIR)
logging.info('Files in %s will be appended/overwritten' % ', '.join(sorted(set(run_dirs))))
for run_dir in run_dirs:
    util.create_dir_if_not_exists(run_dir)
doms_map = util.SafeDict() #mapping of <scheme, hostname> to <domain object, distance>
doms_new_ip = util.SafeDeque() #domains waiting for obtaining their IP address
doms_new_robots = util.SafeDict() #domains waiting for obtaining robots.txt
doms_ready = util.SafeDict() #domains ready for crawling
doms_bad = util.SafeDict() #domains to stop crawling and remove
doms_dead = util.SafeSet() #dead (removed) domains
raw_hashes = util.SafeUniqueSet() #hashes of raw web pages
txt_hashes = util.SafeUniqueSet() #hashes of processed plaintext
duplicates = util.SafeUniqueSet() #duplicate web page IDs
q_new_urls = util.SafeSet() #new URLs to process
q_url_paths = util.SafeDeque() #paths to files for the downloader
url_sent_count = [0]
processed_counts = {}
if util.THREAD_SWITCH_INTERVAL:
    sys.setswitchinterval(util.THREAD_SWITCH_INTERVAL)
time_start = util.now()
logging.info('Scheduler started')

#document processor communication pipes: downloader --> named pipe --> processor
doc_processor_pipe_ids = []
for processor_id in range(config.DOC_PROCESSOR_COUNT):
    processor_pipe_path = '%s/processor_%d_in' % (config.PIPE_DIR, processor_id)
    util.create_pipe_rewrite_if_exists(processor_pipe_path)
    doc_processor_pipe_ids.append((processor_id, processor_pipe_path))

#handle termination signal
terminate = [False] #termination flag
def sigterm_handler(*args):
    logging.info('Crawling will be terminated')
    terminate[0] = True
signal.signal(signal.SIGTERM, sigterm_handler)

def urls_to_download_generator(url_batch_size):
    #queries all selected domains to achieve a good domain variety
    #gives preference to domains with shorter hostnames and closer to the seed domains
    result = []
    while not terminate[0]:
        if len(result) < config.MAX_URL_SELECT:
            domains_ready_tmp = doms_new_robots.values_() | doms_ready.values_()
            for hostname_len_threshold in config.DOM_SCHED_HOSTNAME_LEN_RANGES:
                for dom_distance_threshold in config.DOM_SCHED_DOM_DISTANCE_RANGES:
                    dom_count1 = dom_count2 = url_count = 0
                    for domain in domains_ready_tmp:
                        if domain and len(domain.get_host()) <= hostname_len_threshold:
                            dom_count1 += 1
                            scheme_host = (domain.get_scheme(), domain.get_host())
                            if doms_map.get_strict_(scheme_host)[1] <= dom_distance_threshold:
                                dom_count2 += 1
                                url_tuples = domain.get_url_tuples_to_download(
                                    max_count=config.MAX_URL_SELECT_PER_DOMAIN)
                                if url_tuples:
                                    url_count += len(url_tuples)
                                    result.extend(url_tuples)
                    logging.debug('url_generator %d URLs to download, %d hostname_len %d, '
                        '%d dom_distance %d' % (url_count, dom_count1, hostname_len_threshold,
                        dom_count2, dom_distance_threshold))
        if len(result) < url_batch_size[0]:
            logging.debug('url_generator delay (URL batch %d/%d)' %
                (len(result), url_batch_size[0]))
            sleep(30) #wait for more good URLs from domains
        else:
            yield result[:config.MAX_URL_SELECT]
            result = result[config.MAX_URL_SELECT:]

def write_urls(initial_url_parts=[]):
    batch_id = dead_doms_written = 0
    if initial_url_parts:
        batch_len = len(initial_url_parts)
        url_batch = [' '.join(x) for x in initial_url_parts]
        urls_and_dead_doms_path = '%s/%d' % (config.URL_DIR, batch_id)
        with io.open(urls_and_dead_doms_path, mode='wt', encoding='utf-8',
                buffering=util.BUFSIZE) as urls_and_dead_doms_file:
            urls_and_dead_doms_file.write('%s\n' % '\n'.join(url_batch))
        q_url_paths.append_(urls_and_dead_doms_path)
        logging.debug('%d initial URL records written' % batch_len)
        batch_id += 1
        url_sent_count[0] += batch_len
    url_batch = []
    url_batch_size = [config.MIN_URL_SELECT_START]
    url_batch_sizes = list(reversed(util.increasing_range(start=config.MIN_URL_SELECT_START,
        end=config.MIN_URL_SELECT, step_count=10, repeat_start_count=10))) #cold start
    url_generator = urls_to_download_generator(url_batch_size)
    #write URLs to download and domains to remove to files
    while not terminate[0]:
        #get dead domains
        while True:
            try:
                scheme, host = doms_dead.pop_()
            except KeyError:
                break #no more dead domains
            url_batch.append('%s %s %s' % (util.MSG_DEAD_DOMAIN, scheme, host))
            dead_doms_written += 1
        #get URLs to download
        try:
            urls_to_download = next(url_generator) #blocks until url_batch_size URLs available
        except StopIteration:
            break #the URL generator was terminated
        url_batch.extend([' '.join(x) for x in urls_to_download])
        batch_len = len(url_batch)
        if batch_len >= url_batch_size[0]:
            urls_and_dead_doms_path = '%s/%d' % (config.URL_DIR, batch_id)
            with io.open(urls_and_dead_doms_path, mode='at', encoding='utf-8',
                    buffering=util.BUFSIZE) as urls_and_dead_doms_file:
                urls_and_dead_doms_file.write('%s\n' % '\n'.join(url_batch))
            q_url_paths.append_(urls_and_dead_doms_path)
            logging.debug('%d URL records written' % batch_len)
            batch_id += 1
            url_sent_count[0] += batch_len
            url_batch = []
            if url_batch_sizes:
                url_batch_size[0] = url_batch_sizes.pop()
    logging.debug('%d URLs and %d dead domains written' % (url_sent_count[0], dead_doms_written))

def write_to_downloader(downloader_write_fileno):
    with io.open(downloader_write_fileno, mode='wt', encoding='utf-8', buffering=1,
            newline='\n') as downloader_write_stream, \
            io.open('%s/url_batches_sent' % config.URL_DIR, mode='wt', encoding='utf-8',
            buffering=1, newline='\n') as batches_sent_file:
        for processor_id, processor_pipe_path in doc_processor_pipe_ids:
            downloader_write_stream.write('%d %s\n' %
                (processor_id, processor_pipe_path)) #line buffered
        while not terminate[0]:
            try:
                urls_and_dead_doms_path = q_url_paths.popleft_()
            except IndexError:
                sleep(10) #wait for URL batches
                continue
            downloader_write_stream.write('%s\n' % urls_and_dead_doms_path) #line buffered
            batches_sent_file.write('%s\n' % urls_and_dead_doms_path) #line buffered
        #propagate the termination message to the downloader
        downloader_write_stream.write('%s\n' % util.MSG_TERMINATE) #line buffered

def read_from_downloader(downloader_read_fileno):
    robots_read = 0
    with io.open(downloader_read_fileno, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
            newline='\n') as downloader_read_stream, \
            gzip.open('%s/robots_done.gz' % config.ROBOTS_DIR, mode='at', encoding='utf-8',
            errors='ignore') as robots_done_file:
        while True:
            msg = downloader_read_stream.readline().rstrip() #blocks until read
            #process a robots.txt message
            if msg.startswith(util.MSG_ROBOT_FILE) or msg.startswith(util.MSG_ROBOT_EMPTY) \
                    or msg.startswith(util.MSG_ROBOT_FAIL):
                if msg.startswith(util.MSG_ROBOT_FILE):
                    #read robots.txt from a file
                    scheme, host, robot_path = msg.split(' ')[1:]
                    with io.open(robot_path, mode='rt', encoding='utf-8',
                            buffering=util.BUFSIZE) as robots_file:
                        robot_body = robots_file.read()
                    robots_done_file.write('%s %d\n%s' % (robot_path, len(robot_body), robot_body))
                    util.remove_file_if_exists(robot_path)
                    robots_read += 1
                else:
                    #no robots (a fail flag instead of a body)
                    robot_body, scheme, host = msg.split(' ')
                try:
                    domain = doms_map.get_strict_((scheme, host))[0]
                except KeyError:
                    #robots for a non-existing domain => create a new domain
                    domain = Domain((scheme, host))
                    domain.add_new_paths({'/'})
                    doms_new_ip.append_(domain)
                    other_scheme = 'http' if scheme == 'https' else 'https'
                    try:
                        new_dom_distance = doms_map.get_strict_((other_scheme, host))[1]
                    except KeyError:
                        new_dom_distance = 0
                        logging.debug('No domain distance for robots: %s://%s' % (scheme, host))
                    doms_map.set_((scheme, host), (domain, new_dom_distance))
                if domain:
                    domain.set_robots(robot_body)
            #detect a termination response, wait for that
            elif msg.startswith(util.MSG_TERMINATE):
                logging.debug('The downloader was terminated')
                break
    logging.debug('%d robots read' % robots_read)

def read_from_processor(processor_id, processor_read_fileno):
    processor_counts = processed_counts[processor_id]
    redirections_read = 0
    docmeta_done_path = '%s/doc_meta_done-%d.gz' % (config.DOC_META_DIR, processor_id)
    with io.open(processor_read_fileno, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
            newline='\n') as processor_read_stream, \
            gzip.open('%s/redirections_done.gz' % config.REDIRECT_DIR, mode='at', encoding='utf-8',
            errors='ignore') as redirections_done_file:
        while True:
            msg = processor_read_stream.readline() #blocks until read
            #process a redirection file
            if msg.startswith(util.MSG_NEW_URL):
                redirect_path = msg.rstrip().split(' ')[1]
                with io.open(redirect_path, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
                        newline='\n') as redirect_file:
                    redirect_data = redirect_file.read()
                redirections = [x for x in redirect_data.split('\n') if x]
                redirections_read += len(redirections)
                new_url_parts = set()
                for redirect_pair in redirections:
                    src_scheme, src_host, scheme, host, path = redirect_pair.split(' ')
                    new_url_parts.add((src_scheme, src_host, scheme, host, path))
                q_new_urls.update_(new_url_parts)
                redirections_done_file.write(redirect_data)
                util.remove_file_if_exists(redirect_path)
                logging.debug('%d redirections read' % redirections_read)
            #detect a termination response
            elif msg.startswith(util.MSG_TERMINATE):
                logging.debug('The processor has terminated')
                break
            #load processed documents metadata and extracted URLs
            else:
                metadata_path = msg.rstrip()
                if not metadata_path:
                    continue
                new_url_parts, data_read_buffer = [], []
                with io.open(metadata_path, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
                        newline='\n') as metadata_file:
                    while True:
                        doc_metadata_header = metadata_file.readline()
                        if not doc_metadata_header:
                            break
                        data_read_buffer.append(doc_metadata_header)
                        doc_meta_parts = doc_metadata_header.split(' ')
                        wpage_id, scheme, host, raw_len, html_hash, txt_len = doc_meta_parts[:6]
                        txt_hash, token_count, links_size = doc_meta_parts[-3:]
                        raw_len, txt_len, links_size = int(raw_len), int(txt_len), int(links_size)
                        processor_counts['html_count'] += 1
                        processor_counts['raw_size'] += raw_len
                        #check duplicate text
                        if txt_len:
                            try:
                                txt_hashes.add_unique_(int(txt_hash))
                            except util.ItemNotUniqueException:
                                duplicates.add_(int(wpage_id))
                            else:
                                try:
                                    raw_hashes.add_unique_(int(html_hash))
                                except util.ItemNotUniqueException:
                                    duplicates.add_(int(wpage_id))
                                else:
                                    processor_counts['txt_count'] += 1
                                    processor_counts['clean_size'] += txt_len
                                    processor_counts['token_count'] += int(token_count)
                        #update the domain (ignore if not found any more)
                        if raw_len:
                            domain = doms_map.get_((scheme, host), (None, None))[0]
                            if domain:
                                domain.add_size_downloaded(raw_len)
                                if txt_len:
                                    if config.MULTILINGUAL:
                                        is_primary_lang = (doc_meta_parts[6] == 'P')
                                        domain.add_size_cleaned(txt_len, is_primary_lang)
                                    else:
                                        domain.add_size_cleaned(txt_len)
                        #add new links
                        if links_size:
                            links_s = metadata_file.read(links_size)
                            links_tuples = [x.split(' ') for x in links_s.rstrip().split('\n')]
                            for link_scheme, link_host, link_path in links_tuples:
                                new_url_parts.append((scheme, host, link_scheme, link_host,
                                    link_path))
                            data_read_buffer.append(links_s)
                q_new_urls.update_(new_url_parts)
                with gzip.open(docmeta_done_path, mode='at', encoding='utf-8',
                        errors='ignore') as docmeta_done_file:
                    docmeta_done_file.write(''.join(data_read_buffer))
                util.remove_file_if_exists(metadata_path)

def write_duplicates():
    #write IDs of exactly duplicate documents to a file
    def _write_duplicates():
        duplicate_doc_ids = duplicates.get_set_(clear=True)
        if duplicate_doc_ids:
            with io.open('%s/duplicate_ids' % config.PREVERT_DIR, mode='at', encoding='utf-8',
                    buffering=util.BUFSIZE) as duplicates_file:
                duplicates_file.write('%s\n' % '\n'.join(map(str, duplicate_doc_ids)))
    while not terminate[0]:
        while duplicates.len_() < 500 and not terminate[0]:
            sleep(30) #wait for duplicate IDs
        _write_duplicates()
    _write_duplicates()

def process_new_urls():
    new_url_batch_sizes = [config.MIN_NEW_URL_BATCH_SIZE, 100, 50, 20, 10, 5, 2, 2, 1, 1, 1]
    while not terminate[0]:
        if new_url_batch_sizes:
            new_url_batch_size = new_url_batch_sizes.pop()
        while q_new_urls.len_() < new_url_batch_size and not terminate[0]:
            sleep(30) #wait for more new URLs to process
        #group URLs by hosts, determine distances from seed domains
        target_hosts, dom_distances = {}, {}
        url_parts = q_new_urls.get_set_(clear=True)
        for src_scheme, src_host, scheme, host, path in url_parts:
            src_scheme_host, scheme_host = (src_scheme, src_host), (scheme, host)
            #determine the target domain distance
            if not src_scheme and not src_host:
                target_distance = 0 #initial empty source host
            else:
                try:
                    target_distance = dom_distances[src_scheme_host] + 1
                except KeyError:
                    try:
                        src_distance = doms_map.get_strict_(src_scheme_host)[1]
                    except KeyError:
                        src_distance = 0
                        doms_map.set_(src_scheme_host, (None, src_distance))
                        logging.warning('Missing source domain %s://%s set with distance 0' %
                            (scheme, host))
                    dom_distances[src_scheme_host] = src_distance
                    target_distance = src_distance + 1
            try:
                current_distance = dom_distances[scheme_host]
            except KeyError:
                dom_distances[scheme_host] = target_distance
            else:
                if target_distance < current_distance:
                    dom_distances[scheme_host] = target_distance
            #aggregate new paths by target hosts
            if dom_distances[scheme_host] <= config.MAX_DOMAIN_DISTANCE:
                try:
                    target_hosts[scheme_host].append(path)
                except KeyError:
                    target_hosts[scheme_host] = [path]
            else:
                logging.debug('Ignoring new paths at %s://%s (distance %d)' %
                    (scheme_host + (dom_distances[scheme_host],)))
        #create new domains, update existing domains, add new paths
        removed_dom_paths = []
        for scheme_host, paths in target_hosts.items():
            try:
                domain, distance = doms_map.get_strict_(scheme_host)
            except KeyError:
                #new domain
                domain = Domain(scheme_host)
                domain.add_new_paths(paths + ['/'])
                doms_new_ip.append_(domain)
                doms_map.set_(scheme_host, (domain, dom_distances[scheme_host]))
            else:
                #existing domain
                new_distance = min(distance, dom_distances[scheme_host])
                if domain:
                    domain.add_new_paths(paths)
                    if new_distance < distance:
                        doms_map.set_(scheme_host, (domain, new_distance))
                else:
                    if doms_ready.in_(scheme_host):
                        #wake up and update a sleeping idle domain
                        logging.debug('Waking up sleeping idle %s://%s' % scheme_host)
                        domain = Domain.load_from_file(scheme_host, delete_file=True)[0]
                        domain.add_new_paths(paths)
                        doms_map.set_(scheme_host, (domain, new_distance))
                        doms_ready.set_(scheme_host, domain)
                    else:
                        #update a removed domain
                        logging.debug('Updating removed %s://%s' % scheme_host)
                        if new_distance < distance:
                            doms_map.set_(scheme_host, (None, new_distance))
                        removed_dom_paths.extend('%s://%s %s\n' %
                            (scheme_host + (' '.join(paths),)))
        #store paths from removed domains
        if removed_dom_paths:
            with gzip.open('%s/removed_domain_paths.gz' % config.URL_DIR, mode='at',
                    encoding='utf-8', errors='ignore') as removed_dom_paths_file:
                removed_dom_paths_file.write(''.join(removed_dom_paths))
        logging.debug('%d URLs in %d domains added, %d removed domain paths dumped' %
            (len(url_parts), len(target_hosts), len(removed_dom_paths)))

def ip_resolve():
    with io.open('%s/dns_table' % config.URL_DIR, mode='at', encoding='utf-8',
            buffering=util.BUFSIZE) as dns_file:
        if config.DNS_TABLE_PATH:
            with io.open(config.DNS_TABLE_PATH, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
                    newline='\n') as dns_src_file:
                dns_lines = [x for x in dns_src_file.read().split('\n') if x]
            dns_table = {}
            for dns_line in dns_lines:
                try:
                    scheme, host, ip = dns_line.split(' ')
                except ValueError:
                    host, ip = dns_line.split(' ')
                    scheme = 'http'
                dns_table[(scheme, host)] = ip
                dns_file.write('%s %s %s\n' % (scheme, host, ip))
            logging.debug('%d host name to IP mappings loaded' % len(dns_table))
        else:
            dns_table = {}
        resolved_count = total_count = 0
        while not terminate[0]:
            #new domains (waiting for IP) -> new domains (waiting for robots)
            try:
                domain = doms_new_ip.popleft_()
            except IndexError:
                sleep(10) #no new domain
            else:
                scheme_host = (domain.get_scheme(), domain.get_host())
                try:
                    ip = dns_table.pop(scheme_host)
                except KeyError:
                    ip = get_ip(domain.get_host()) #blocks
                    dns_file.write('%s %s %s\n' % (scheme_host + (ip,)))
                    resolved_count += 1
                if domain.set_ip(ip):
                    doms_new_robots.set_(scheme_host, domain)
                else:
                    doms_bad.set_(scheme_host, domain)
                total_count += 1
        logging.debug('%d/%d host name to IP resolved' % (resolved_count, total_count))

"Update domain sets: select good domains for crawling, remove bad domains, evaluate efficiency"
def update_domains():
    #log domains' state
    ready_path_count = sum((domain.get_new_path_count() for domain in doms_ready.values_(
        ) if domain is not None))
    logging.debug('Before: %d new IP, %d new robots, %d ready, %d bad, %d dead; %d new paths' %
        (doms_new_ip.len_(), doms_new_robots.len_(), doms_ready.len_(), doms_bad.len_(),
        doms_dead.len_(), ready_path_count))
    #new (waiting for robots) -> bad (remove) | ready (can be crawled)
    robots_to_ready = robots_to_bad = 0
    for scheme_host, domain in doms_new_robots.items_():
        if domain.is_bad():
            doms_new_robots.del_(scheme_host)
            doms_bad.set_(scheme_host, domain)
            robots_to_bad += 1
        elif domain.is_robot_ready():
            doms_new_robots.del_(scheme_host)
            doms_ready.set_(scheme_host, domain)
            robots_to_ready += 1
    #ready bad/inefficient/max_docs -> bad (remove) | idle (move from RAM to a file)
    still_sleeping = ready_to_sleep = ready_to_bad_bad = ready_to_bad_eff = ready_to_bad_size = 0
    for scheme_host, domain in doms_ready.items_():
        remove = False
        if domain is None:
            still_sleeping += 1 #no change
        elif domain.is_bad():
            remove = True
            ready_to_bad_bad += 1
        elif domain.is_idle():
            distance = doms_map.get_strict_(scheme_host)[1]
            domain.save_to_file(distance)
            doms_map.set_(scheme_host, (None, distance))
            doms_ready.set_(scheme_host, None)
            ready_to_sleep += 1
        elif not domain.is_efficient():
            remove = True
            ready_to_bad_eff += 1
        elif config.MAX_DOCS_CLEANED and domain.get_count_cleaned() >= config.MAX_DOCS_CLEANED:
            remove = True
            ready_to_bad_size += 1
        if remove:
            doms_ready.del_(scheme_host)
            doms_bad.set_(scheme_host, domain)
    #bad -> remove
    with gzip.open('%s/domain_bad.gz' % config.IGNORED_DIR, mode='ab') as domain_bad_file:
        while True:
            try:
                scheme_host, domain = doms_bad.popitem_()
            except KeyError:
                break #no more bad domains
            distance = doms_map.get_strict_(scheme_host)[1]
            doms_map.set_(scheme_host, (None, distance))
            doms_dead.add_(scheme_host)
            if domain:
                domain_bad_file.write(domain.save_to_bytes(distance)) #encoded
    #log domains' state
    ready_path_count = sum((domain.get_new_path_count() for domain in doms_ready.values_(
        ) if domain is not None))
    logging.debug('After:  %d new IP, %d new robots, %d ready, %d bad, %d dead; %d new paths' %
        (doms_new_ip.len_(), doms_new_robots.len_(), doms_ready.len_(), doms_bad.len_(),
        doms_dead.len_(), ready_path_count))
    logging.debug('Detail: %d robots->ready, %d ready->sleep, %d sleeping, %d robots->bad, '
        '%d ready->bad-bad, %d ready->bad-yield, %d ready->bad-max-docs' %
        (robots_to_ready, ready_to_sleep, still_sleeping, robots_to_bad, ready_to_bad_bad,
        ready_to_bad_eff, ready_to_bad_size))

def update_domains_loop():
    #check domains in intervals growing up to UPDATE_DOMAINS_PERIOD
    update_domains_period, update_domains_periods = 20, list(reversed(util.increasing_range(
        start=10, end=config.UPDATE_DOMAINS_PERIOD, step_count=20, repeat_start_count=5)))
    period_start = util.now()
    while not terminate[0]:
        if util.seconds_left(period_start, update_domains_period) < 2:
            period_start = util.now()
            update_domains()
            if update_domains_periods:
                update_domains_period = update_domains_periods.pop()
        sleep(30)

def load_state(domains_path, raw_hashes_path, txt_hashes_path):
    logging.info('Loading state: %s %s %s' % (domains_path, raw_hashes_path, txt_hashes_path))
    domain_count = 0
    with gzip.open(domains_path, mode='rb') as domain_file:
        while True:
            chunk_size = domain_file.readline()
            if not chunk_size:
                break
            domain, distance = Domain.load_from_bytes(domain_file.read(int(chunk_size)))
            doms_map.set_((domain.get_scheme(), domain.get_host()), (domain, distance))
            doms_new_ip.append_(domain)
            domain_count += 1
    logging.debug('%d domains loaded' % domain_count)
    with gzip.open(raw_hashes_path, mode='rb') as raw_hash_file:
        raw_hashes.update_(util.unpack_ints(raw_hash_file.read()))
    logging.debug('%d raw hashes loaded' % raw_hashes.len_())
    with gzip.open(txt_hashes_path, mode='rb') as txt_hash_file:
        txt_hashes.update_(util.unpack_ints(txt_hash_file.read()))
    logging.debug('%d txt hashes loaded' % txt_hashes.len_())
    logging.info('State loaded: %d domains, %d raw hashes, %d txt hashes' %
        (domain_count, raw_hashes.len_(), txt_hashes.len_()))

def save_state():
    state_id = util.now().strftime('%Y%m%d%H%M%S')
    logging.info('Saving state %s' % state_id)
    domain_count = 0
    try:
        with gzip.open('%s/domains_%s.gz' % (config.SAVE_DIR, state_id), mode='wb') as domain_file:
            for scheme_host in doms_map.keys_():
                domain, distance = doms_map.get_strict_(scheme_host)
                if domain:
                    domain_bs = domain.save_to_bytes(distance)
                else:
                    domain_bs = Domain.save_empty_domain_to_bytes(scheme_host, distance)
                domain_file.write(b'%d\n' % len(domain_bs)) #encoded
                domain_file.write(domain_bs) #encoded
                domain_count += 1
        with gzip.open('%s/raw_hashes_%s.gz' % (config.SAVE_DIR, state_id), mode='wb') \
                as raw_hash_file:
            raw_hashes_copy = raw_hashes.get_set_()
            raw_hash_file.write(util.pack_ints(raw_hashes_copy)) #encoded
        with gzip.open('%s/txt_hashes_%s.gz' % (config.SAVE_DIR, state_id), mode='wb') \
                as txt_hash_file:
            txt_hashes_copy = txt_hashes.get_set_()
            txt_hash_file.write(util.pack_ints(txt_hashes_copy)) #encoded
    except Exception as e:
        logging.warning('Unable to save state: %s' % e)
    else:
        logging.info('State saved: %d domains, %d raw hashes, %d txt hashes' %
            (domain_count, len(raw_hashes_copy), len(txt_hashes_copy)))

def save_state_loop():
    period_start = util.now()
    while not terminate[0]:
        if util.seconds_left(period_start, config.SAVE_PERIOD) < 2:
            period_start = util.now()
            save_state()
        sleep(30)

def print_info():
    html_count = raw_size = txt_count = clean_size = token_count = 0
    for processed_count in processed_counts.values():
        html_count += processed_count['html_count']
        raw_size += processed_count['raw_size']
        txt_count += processed_count['txt_count']
        clean_size += processed_count['clean_size']
        token_count += processed_count['token_count']
    logging.info('INFO domains IP %d, robots %d, ready %d, bad %d; '
        '%d HTTP, %d HTML (%d MB), %d txt (%d MB, ~%d M words)' %
        (doms_new_ip.len_(), doms_new_robots.len_(), doms_ready.len_(), doms_bad.len_(),
        url_sent_count[0], html_count, raw_size // 1024**2, txt_count, clean_size // 1024**2,
        token_count // 1000**2))

def print_sizes():
    try:
        logging.info('SIZES [MB]: doms_map: %d, doms_new_ip: %d, doms_new_robots: %d, '
            'doms_ready: %d, doms_bad: %d, doms_dead: %d, raw_hashes: %d, txt_hashes: %d, '
            'duplicates: %d, q_new_urls: %d, q_url_paths: %d' %
            (util.get_size_mb(doms_map), util.get_size_mb(doms_new_ip),
            util.get_size_mb(doms_new_robots), util.get_size_mb(doms_ready),
            util.get_size_mb(doms_bad), util.get_size_mb(doms_dead), util.get_size_mb(raw_hashes),
            util.get_size_mb(txt_hashes), util.get_size_mb(duplicates),
            util.get_size_mb(q_new_urls), util.get_size_mb(q_url_paths)))
    except Exception as e:
        logging.info('SIZES: (could not determine sizes)')
        logging.debug('SIZES: Exception %s' % e)

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

#load a saved state and seed URLs or URL file paths
url_parts, url_parts_direct = [], []
wpage_paths_file_path = None
with io.open(sys.stdin.fileno(), mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
        newline='\n') as input_read_stream:
    input_items = input_read_stream.read().rstrip().split('\n') #stdin in memory
    logging.debug('%d lines read in memory' % len(input_items))
    for arg in sys.argv[1:]:
        if arg.startswith('--state-files='):
            domains_path, raw_hashes_path, txt_hashes_path = arg.split('=', 1)[1].split(',')
            load_state(domains_path, raw_hashes_path, txt_hashes_path)
        elif arg.startswith('--wpage-paths-file='):
            wpage_paths_file_path = arg.split('=', 1)[1]
            if not os.path.exists(wpage_paths_file_path):
                logging.critical('Wpage list %s does not exist' % wpage_paths_file_path)
                sys.exit(2)
    if '--seed-triples' in sys.argv:
        #load 3-tuples <scheme, host, path>
        logging.info('Loading seed tuples <scheme, host, path> from stdin into Domains')
        logging.debug('Loading into Domains')
        for line in input_items:
            scheme, host, path = line.split(' ')
            url_parts.append((None, None, scheme, host, path)) #no source host
    elif '--old-tuples' in sys.argv:
        #load 5-tuples <URL, scheme, host, path, IP> (assuming from the previous url/urls_waiting)
        #without duplicate path checking in Domains since these paths were checked
        #in a previous run which state is being loaded
        logging.info('Loading seed tuples <URL, scheme, host, path, IP> from stdin')
        logging.debug('Loading without checks in Domains')
        for line in input_items:
            url, scheme, host, path, ip = line.split(' ')
            robot_flag = util.ROBOT_FLAG if path == '/robots.txt' else ''
            url_parts_direct.append((scheme, host, path, ip, robot_flag))
    else:
        #load URLs and split to 3-tuples <scheme, host, path>
        logging.info('Loading seed URLs from stdin')
        logging.debug('Loading into Domains')
        line_count = 0
        for url in input_items:
            url = url.strip()
            if url and url[0] != '#':
                if url[:4] != 'http':
                    url = 'http://%s' % url
                scheme, host, path = url_split(url)
                url_parts.append((None, None, scheme, host, path)) #no source host
            line_count += 1
            if line_count % 1000000 == 0:
                logging.debug('%d lines processed' % line_count)
        logging.debug('%d lines processed' % line_count)
    if url_parts:
        q_new_urls.update_(url_parts)
        logging.debug('Domains will be updated with %d URLs' % len(url_parts))
    if url_parts_direct:
        logging.debug('Downloader will be updated with %d URLs' % len(url_parts_direct))

#start URL processing and domain routines
t_process_new_urls = util.LogThread(logging, target=process_new_urls, name='process_new_urls')
t_process_new_urls.start()
t_write_urls = util.LogThread(logging, target=write_urls, name='write_urls',
    args=(url_parts_direct,))
t_write_urls.start()
t_ip_resolve = util.LogThread(logging, target=ip_resolve, name='ip_resolve')
t_ip_resolve.start()
t_update_domains = util.LogThread(logging, target=update_domains_loop, name='update_domains')
t_update_domains.start()

#start the downloader
downloader_exec = config.DOWNLOADER_EXEC
if wpage_paths_file_path:
    downloader_exec.append('--wpage-paths-file=%s' % wpage_paths_file_path)
p_downloader = subprocess.Popen(downloader_exec, bufsize=1,
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
t_downloader_write = util.LogThread(logging, target=write_to_downloader,
    name='write_to_downloader', args=(p_downloader.stdin.fileno(),))
t_downloader_write.start()
t_downloader_read = util.LogThread(logging, target=read_from_downloader,
    name='read_from_downloader', args=(p_downloader.stdout.fileno(),))
t_downloader_read.start()

#start web page processors
t_processors_read = []
p_processors_stdin = []
for processor_id, processor_pipe_path in doc_processor_pipe_ids:
    p_processor = subprocess.Popen(config.PROCESSOR_EXEC, bufsize=1,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
    processed_counts[processor_id] = {'html_count': 0, 'raw_size': 0, 'txt_count': 0,
        'clean_size': 0, 'token_count': 0}
    p_processor.stdin.write('%d %s\n' % (processor_id, processor_pipe_path))
    p_processor.stdin.flush()
    t_processor_read = util.LogThread(logging, target=read_from_processor,
        name='read_from_processor %d' % processor_id,
        args=(processor_id, p_processor.stdout.fileno()))
    t_processor_read.start()
    t_processors_read.append(t_processor_read)
    p_processors_stdin.append(p_processor.stdin)

#start other threads
t_duplicates = util.LogThread(logging, target=write_duplicates, name='write_duplicates')
t_duplicates.start()
if config.INFO_PERIOD or config.SIZES_PERIOD:
    t_info = util.LogThread(logging, target=print_info_loop, name='print_info_loop')
    t_info.start()
if config.SAVE_PERIOD:
    t_save = util.LogThread(logging, target=save_state_loop, name='save_state_loop')
    t_save.start()
logging.info('State initialized/loaded, downloader and document processors started')
save_state()

#wait for termination, then stop all processes
while not terminate[0] and (not config.MAX_RUN_TIME or util.seconds_left(
        time_start, config.MAX_RUN_TIME) > 0):
    sleep(30)
logging.info('All subprocesses will be stopped')
terminate[0] = True
logging.info('Runtime: %d s' % util.seconds_total(util.now() - time_start))
if config.INFO_PERIOD:
    print_info()
if config.SIZES_PERIOD:
    print_sizes()
save_state()
t_update_domains.join()
t_process_new_urls.join()
t_write_urls.join()
t_downloader_write.join()
t_downloader_read.join()
for p_processor_stdin in p_processors_stdin:
    #propagate the termination message to the processor
    p_processor_stdin.write('%s\n' % util.MSG_TERMINATE)
    p_processor_stdin.flush()
for t_processor_read in t_processors_read:
    t_processor_read.join()
t_duplicates.join()
if config.INFO_PERIOD or config.SIZES_PERIOD:
    t_info.join()
if config.SAVE_PERIOD:
    t_save.join()
logging.info('Scheduler finished')
