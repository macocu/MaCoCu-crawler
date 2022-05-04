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

USAGE = """
Reprocess arc or wpage stdin using document processors
applying configuration set in util/config.py.
Useful for re-processing previously crawled raw arc or wpage data.
Usage with an arc input,
    e.g. to re-process raw downloaded data:
    zcat arc/*.arc.gz | pypy3 reprocess.py --arc [OUT_PREFIX] \
        [DOC_PROCESSOR_COUNT] [MAX_WPAGE_BATCH_SIZE] [--saved-hashes=PATH,PATH]
Usage with a wpage input,
    e.g. to continue processing the rest of wpage data after stopping the crawler:
    cat wpage/* | pypy3 reprocess.py --wpage [OUT_PREFIX] \
        [DOC_PROCESSOR_COUNT] [MAX_WPAGE_BATCH_SIZE]
OUT_PREFIX is the output directory prefix (<OUT_PREFIX>_run will be created),
DOC_PROCESSOR_COUNT overrides config.DOC_PROCESSOR_COUNT if specified,
MAX_WPAGE_BATCH_SIZE overrides config.MAX_WPAGE_BATCH_SIZE if specified.
Use --saved-hashes=path/to/raw_hashes.gz,path/to/txt_hashes.gz to initialize
the hashes of raw data and plaintext by values saved when running the crawler.
"""

import io, sys, gzip
import subprocess

import util
from util import config
from util.http import url_split

#initialise
if len(sys.argv) < 2 or sys.argv[1] not in ('--arc', '--wpage', '--arc-old') \
        or '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
    sys.stderr.write(USAGE)
    sys.exit(1)
ARC_INPUT = sys.argv[1] == '--arc'
WPAGE_INPUT = sys.argv[1] == '--wpage'
ARC_OLD_INPUT = sys.argv[1] == '--arc-old'
out_prefix = 'reprocess_%s_' % util.now().strftime('%Y%m%d%H%M%S')
if len(sys.argv) > 2:
    out_prefix = sys.argv[2].replace(' ', '_')
doc_proc_count = config.DOC_PROCESSOR_COUNT
if len(sys.argv) > 3:
    doc_proc_count = int(sys.argv[3])
max_wpage_batch_size = config.MAX_WPAGE_BATCH_SIZE
if len(sys.argv) > 4:
    max_wpage_batch_size = int(sys.argv[4])
run_dirs = [out_prefix + x for x in (config.RUN_DIR, config.LOG_DIR, config.PIPE_DIR,
    config.WPAGE_DIR, config.DOC_META_DIR, config.ARC_DIR, config.PREVERT_DIR, config.IGNORED_DIR)]
RUN_DIR, LOG_DIR, PIPE_DIR, WPAGE_DIR, DOC_META_DIR, ARC_DIR, PREVERT_DIR, IGNORED_DIR = run_dirs
util.create_dir_if_not_exists(RUN_DIR)
util.create_dir_if_not_exists(LOG_DIR)
import logging
log_stream = io.open('%s/reprocess.log' % LOG_DIR, mode='at',
    encoding='utf-8', errors='ignore', buffering=config.LOG_BUFFERING)
logging.basicConfig(stream=log_stream, level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logging.info('Reprocessing arc from stdin to %s with %d document processors' %
    (PREVERT_DIR, doc_proc_count))
logging.info('Files in %s will be appended/overwritten' % ', '.join(sorted(set(run_dirs))))
for run_dir in run_dirs:
    util.create_dir_if_not_exists(run_dir)
PROCESSOR_EXEC = config.PROCESSOR_EXEC + ['reprocessing', out_prefix.encode('utf-8')]

arc_queue = util.SafeDeque()
processed_counts = {}
raw_hashes = util.SafeUniqueSet() #hashes of raw web pages
txt_hashes = util.SafeUniqueSet() #hashes of processed plaintext
duplicates = util.SafeUniqueSet() #duplicate web page IDs
for arg in sys.argv:
    if arg.startswith('--saved-hashes='):
        raw_hashes_path, txt_hashes_path = arg[15:].split(',')
        with gzip.open(raw_hashes_path, mode='rb') as raw_hash_file:
            raw_hashes.update_(util.unpack_ints(raw_hash_file.read()))
        logging.debug('%d raw hashes loaded' % raw_hashes.len_())
        with gzip.open(txt_hashes_path, mode='rb') as txt_hash_file:
            txt_hashes.update_(util.unpack_ints(txt_hash_file.read()))
        logging.debug('%d txt hashes loaded' % txt_hashes.len_())

def read_from_processor(processor_id, processor_read_fileno):
    processor_counts = processed_counts[processor_id]
    docmeta_done_path = '%s/doc_meta_done-%d' % (DOC_META_DIR, processor_id)
    with io.open(processor_read_fileno, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
            newline='\n') as processor_read_stream:
        while True:
            msg = processor_read_stream.readline() #blocks until read
            #detect a termination response
            if msg.startswith(util.MSG_TERMINATE):
                logging.debug('The processor has terminated')
                break
            #load processed documents metadata and extracted URLs
            else:
                metadata_path = msg.rstrip()
                if not metadata_path:
                    continue
                data_read_buffer = []
                with io.open(metadata_path, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
                        newline='\n') as metadata_file:
                    while True:
                        doc_metadata_header = metadata_file.readline()
                        if not doc_metadata_header:
                            break
                        data_read_buffer.append(doc_metadata_header)
                        doc_meta_parts = doc_metadata_header.split(' ')
                        wpage_id, _, _, raw_len, html_hash, txt_len = doc_meta_parts[:6]
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
                        #read wpage links to seek to the next document header
                        if links_size:
                            data_read_buffer.append(metadata_file.read(links_size))
                with io.open(docmeta_done_path, mode='at', encoding='utf-8',
                        buffering=util.BUFSIZE) as docmeta_done_file:
                    docmeta_done_file.write(''.join(data_read_buffer))
                util.remove_file_if_exists(metadata_path)

#set up and start document processor communication
t_processors_read = []
processors_pipe_paths = {}
processors_stdin = {}
for processor_id in range(doc_proc_count):
    processor_pipe_path = '%s/processor_%d_in' % (PIPE_DIR, processor_id)
    util.create_pipe_rewrite_if_exists(processor_pipe_path)
    p_processor = subprocess.Popen(PROCESSOR_EXEC, bufsize=1,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
    processed_counts[processor_id] = {'html_count': 0, 'raw_size': 0, 'txt_count': 0,
        'clean_size': 0, 'token_count': 0}
    processor_stdin_stream = io.open(p_processor.stdin.fileno(), mode='wt', encoding='utf-8',
        buffering=1, newline='\n')
    processor_stdin_stream.write('%d %s\n' % (processor_id, processor_pipe_path)) #line buffered
    processors_pipe_paths[processor_id] = processor_pipe_path
    processors_stdin[processor_id] = processor_stdin_stream
    t_processor_read = util.LogThread(logging, target=read_from_processor,
        name='read_from_processor %d' % processor_id,
        args=(processor_id, p_processor.stdout.fileno()))
    t_processor_read.start()
    t_processors_read.append(t_processor_read)

#This script is a wrapper for processor.py. In terms of communication with the wrapped script
#it behaves like both downloader and scheduler. It sends web page data read from an arc input
#and receives metadata of processed documents.
processor_id = input_count = bytes_read = wpage_batch_id = 0
wpage_batch, wpage_batch_len = [], 0
stdin = io.open(sys.stdin.fileno(), mode='rb', buffering=64*1024**2)
terminate = False
while not terminate:
    #read arc record header
    lines_skipped = 0
    while True:
        input_header_bs = stdin.readline() #encoded
        if not input_header_bs:
            break
        bytes_read += len(input_header_bs)
        try:
            head_parts = input_header_bs.decode('utf-8', errors='replace').split(' ')
            if ARC_INPUT:
                url, ip, conn_time, mime_type, header_len, body_len, wpage_id = head_parts
            elif WPAGE_INPUT:
                wpage_id, url, scheme, host, path, ip, conn_time, header_len, body_len = head_parts
            elif ARC_OLD_INPUT:
                url, ip, conn_time, mime_type, header_len, wpage_id = head_parts
                body_len = 0 #must be dealt with later
            wpage_id, header_len, body_len = int(wpage_id), int(header_len), int(body_len)
            if not url.startswith('http'):
                raise ValueError()
        except ValueError:
            lines_skipped += 1
            if lines_skipped <= 1000000:
                continue
            else:
                input_header_bs = '' # => terminate
                logging.warning('%d lines skipped while looking for arc header before input byte '
                    ' %d => terminate' % (lines_skipped, bytes_read))
        break
    if input_header_bs:
        if lines_skipped:
            logging.warning('%d lines skipped while looking for arc header before input byte %d' %
                (lines_skipped, bytes_read))
        #read arc record body (i.e. HTTP response)
        header_bs = stdin.read(header_len)
        body_bs = stdin.read(body_len)
        if ARC_OLD_INPUT:
            try:
                header_bs, body_bs = header_bs.split(b'<', 1)
            except Exception as e:
                logging.warning('could not split a record to a header and a body at input byte '
                    '%d: %s' % (bytes_read, e))
                continue
            header_len, body_len = len(header_bs), len(body_bs)
            bytes_read += header_len + body_len
        else:
            header_bs_len, body_bs_len = len(header_bs), len(body_bs)
            if header_bs_len < header_len or body_bs_len < body_len:
                logging.warning('could not parse HTTP response at input byte %d' % bytes_read)
                continue
            bytes_read += header_bs_len + body_bs_len
        #write batches of raw encoded web pages to files and send the file paths to the processor
        if ARC_INPUT or ARC_OLD_INPUT:
            try:
                scheme, host, path = url_split(url)
            except ValueError:
                logging.warning('url_split fail with "%s", skipping' % url)
                continue
        input_count += 1
        wpage_header = '%d %s %s %s %s %s %s %d %d' % \
            (wpage_id, url, scheme, host, path, ip, conn_time, header_len, body_len + 1)
        wpage_batch.append(b'%s\n%s%s\n' % (wpage_header.encode('utf-8'), header_bs, body_bs))
        wpage_batch_len += 1
    else:
        terminate = True
    if wpage_batch_len >= max_wpage_batch_size or terminate and wpage_batch_len > 0:
        wpage_batch_id += 1
        wpage_path = '%s/%d' % (WPAGE_DIR, wpage_batch_id)
        with io.open(wpage_path, mode='wb', buffering=util.BUFSIZE) as web_page_file:
            web_page_file.write(b''.join(wpage_batch)) #encoded
        with io.open(processors_pipe_paths[processor_id], mode='wt', encoding='utf-8',
                buffering=1) as processor_write_stream:
            processor_write_stream.write('%s\n' % wpage_path) #line buffered
        logging.debug('%d web pages written to processor %d' % (wpage_batch_len, processor_id))
        wpage_batch, wpage_batch_len = [], 0
        processor_id += 1 #document processors take turns
        if processor_id >= doc_proc_count:
            processor_id = 0
    if input_count % 10000 == 0:
        logging.debug('%d arc records read (%.1f GB), %d files sent to process' %
            (input_count, bytes_read / 1024.0**3, wpage_batch_id))
logging.info('%d arc records read (%.1f GB), %d files sent to process, finished' %
    (input_count, bytes_read / 1024.0**3, wpage_batch_id))

#end of stdin => termination, wait for doc processor reading threads
for processor_id in range(doc_proc_count):
    processors_stdin[processor_id].write('%s\n' % util.MSG_TERMINATE) #line buffered
    processors_stdin[processor_id].close()
for t_processor_read in t_processors_read:
    t_processor_read.join()

#write IDs of exactly duplicate documents to a file
logging.debug('Writing duplicate document IDs')
duplicate_doc_ids = duplicates.get_set_(clear=True)
with io.open('%s/duplicate_ids' % PREVERT_DIR, mode='at', encoding='utf-8',
        buffering=util.BUFSIZE) as duplicates_file:
    duplicates_file.write('%s\n' % '\n'.join(map(str, duplicate_doc_ids)))
logging.info('Writing duplicate document IDs done')

#print some info
html_count = raw_size = txt_count = clean_size = token_count = 0
for processed_count in processed_counts.values():
    html_count += processed_count['html_count']
    raw_size += processed_count['raw_size']
    txt_count += processed_count['txt_count']
    clean_size += processed_count['clean_size']
    token_count += processed_count['token_count']
logging.info('Reprocessing finished: %d html (%d MB), %d txt (%d MB, ~%d M words)' %
    (html_count, raw_size // 1024**2, txt_count, clean_size // 1024**2, token_count // 1000**2))
