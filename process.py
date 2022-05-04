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
Processor of downloaded data
Extracts plaintext and URLs from downloaded data. I/O:
- read the processor ID from the standard input (from the scheduler)
- read paths to files with downloaded data from a pipe input (from the downloader)
- read downloaded data from files (in the original encoding)
- write the plaintext in the prevertical format and the URLs to files (in UTF-8)
- write paths to files with extracted data to the standard output (to the scheduler)
- write ignored URLs (not meeting conditions) and binary file URLs (e.g. pdf) to files
"""

import sys, os, re, io, codecs, gzip
from collections import deque
from select import select
import subprocess
from time import sleep
from threading import Thread

import util
from util import config
if config.UNIGRAM_MODELS:
    from util import trigrams1 as trigrams #unigram hack for CJK #@UnusedImport
else:
    from util import trigrams #@Reimport
from chared.detector import EncodingDetector
from justext import core as justext
import lxml.html
from lxml.etree import LxmlError
from util.http import url_join_rel_norm, url_split_and_check_parts, url_strip, clean_url_display, \
    extract_file_type, extract_last_modified

from justext import __version__ as justext_version
if justext_version < '1.4':
    sys.stderr.write('Error: Justext >= 1.4 required\n')
    sys.exit(1)

CONVERSION_COMMANDS = {
    'doc':  ['antiword', '-t', '-s', '-mUTF-8', '-'],
    'docx': ['util/docx2txt/docx2txt.pl', '-', '-'],
    'odt':  ['util/odf2txt/odf_extract.py'],
    'pdf':  ['pdftotext', '-nopgbrk', '-enc', 'UTF-8', '-q', '-', '-'],
    'ps':   ['ps2ascii', '-q', '-sOutputFile=-'],
}

#strings indicating machine translated content -- based on Jelmer van der Linde's
#https://github.com/paracrawl/cirrus-scripts/blob/master/mt-filter-list.annotated
MT_STRINGS = ('mqtranslate', 'wporg-translate', 'wp-translate', 'qtranslate', 'wporg-translate-css',
    'machine-translated-from', 'qtranslate-x', 'delivered by GTranslate', 'name="translation-stats',
    'data-trp-gettext', 'id="weglot')

#command line arguments for re-processing mode
reprocessing_mode = len(sys.argv) > 1 and sys.argv[1] == 'reprocessing'
out_prefix = ''
if len(sys.argv) > 2:
    out_prefix = sys.argv[2]
RUN_DIR, IGNORED_DIR, ARC_DIR, PREVERT_DIR, REDIRECT_DIR, DOC_META_DIR, LOG_DIR = (
    out_prefix + config.RUN_DIR, out_prefix + config.IGNORED_DIR, out_prefix + config.ARC_DIR,
    out_prefix + config.PREVERT_DIR, out_prefix + config.REDIRECT_DIR,
    out_prefix + config.DOC_META_DIR, out_prefix + config.LOG_DIR)

import logging
log_stream = io.open('%s/processor-%d.log' % (LOG_DIR, os.getpid()), mode='at',
    encoding='utf-8', errors='ignore', buffering=config.LOG_BUFFERING)
logging.basicConfig(stream=log_stream, level=config.LOG_LEVEL, format=config.LOG_FORMAT)

#initialise
processor_id, processor_pipe_path = sys.stdin.readline().rstrip().split(' ')
processor_id = int(processor_id)
q_web_pages = util.SafeDeque() #raw web pages to process
q_redirect = util.SafeDeque() #redirections to process
q_documents = util.SafeDeque() #processed documents
terminate = [False] #termination flag
end_of_input = [False] #end of input flag for the reprocessnig mode
if util.THREAD_SWITCH_INTERVAL:
    sys.setswitchinterval(util.THREAD_SWITCH_INTERVAL)
logging.info('Processor %d started' % processor_id)

#build language dependent models
lang_models, chared_models, justext_wordlists = {}, {}, {}
for lang in config.LANGUAGES:
    #character trigrams from a language sample for language identification
    with io.open('util/lang_samples/%s' % lang, mode='rt', encoding='utf-8',
            buffering=util.BUFSIZE, newline='\n') as lang_sample_file:
        lang_sample = lang_sample_file.read()
    lang_models[lang] = trigrams.Trigram()
    lang_models[lang].parseLines([lang_sample])
    #byte trigrams for character encoding detection
    if config.FORCE_ENCODING is None:
        chared_models[lang] = EncodingDetector.load('%s/%s' % (config.CHARED_MODEL_DIR, lang))
    #Justext wordlist for html boilerplate removal
    if config.SPACE_SEP_TOKENS:
        if config.JUSTEXT_WORDLIST_DIR:
            with io.open('%s/%s' % (config.JUSTEXT_WORDLIST_DIR, lang), mode='rt',
                    encoding='utf-8', buffering=util.BUFSIZE, newline='\n') as wordlist_file:
                wordlist_lines = [l.strip() for l in wordlist_file]
                justext_wordlists[lang] = set(l for l in wordlist_lines if l and l[0] != '#')
        else:
            justext_wordlists[lang] = justext.get_stoplist(lang) #the default justext wordlist
    else:
        justext_wordlists[lang] = set() #no space separable tokens => cannot use a wordlist

def get_lang_difference(lang_model, txt_data):
    data_trigrams = trigrams.Trigram()
    data_trigrams.parseLines([txt_data])
    lang_difference = lang_model - data_trigrams
    return lang_difference

def format_date(timestamp_s):
    if len(timestamp_s) == 14:
        return '%s-%s-%s %s:%s' % (timestamp_s[0:4], timestamp_s[4:6], timestamp_s[6:8],
            timestamp_s[8:10], timestamp_s[10:12])
    logging.warning('Invalid timestamp value: %s' % timestamp_s)
    return timestamp_s

DOC_LEN_RANGES = ((1000000, '1M+'), (100000, '100k-1M'), (10000, '10k-100k'), (5000, '5k-10k'),
    (1000, '1k-5k'), (500, '500-1k'), (100, '100-500'), (0, '0-100'))
def format_doc_len(doc_len):
    for threshold, description in DOC_LEN_RANGES:
        if doc_len >= threshold:
            return description

CONVERSION_TIMEOUT = ['timeout', '-k', '20', '30'] #send TERM/KILL to make sure the process ends
CONVERSION_NICE = ['ionice', '-c', '3', 'nice', '-n', '19'] #the lowest I/O and CPU prioity
def convert_to_txt(command, data_bs):
    logging.debug('convert_to_txt %.0f kB using "%s"' % (len(data_bs) / 1024.0, ' '.join(command)))
    dataIO = io.BytesIO()
    dataIO.write(data_bs)
    dataIO.seek(0)
    try:
        process = subprocess.Popen(CONVERSION_TIMEOUT + CONVERSION_NICE + command,
            bufsize=config.DOC_SIZE_MAX, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        process_output_bs, process_errors_bs = process.communicate(dataIO.read())
    except Exception as e:
        logging.debug('convert_to_txt exception: %s' % e)
        return b''
    finally:
        dataIO.close()
    if process_errors_bs:
        process_errors = process_errors_bs.decode('utf-8', errors='replace')
        logging.debug('convert_to_txt %s stderr: %s' %
            (command[0], process_errors[:200].replace('\n', ' ')))
    try:
        return process_output_bs
    except UnicodeDecodeError:
        logging.debug('convert_to_txt output could not be decoded, ignoring the file')
        return b''

META_ENC1_BS_RE = re.compile(
    b'''<meta\s+http-equiv=['"]?content-type['"]?\s+content=['"]?[^'"]*charset=([^'" >/]+)''', re.I)
META_ENC2_BS_RE = re.compile(
    b'''<meta\s+content=['"]?[^'"]*charset=([^'" >/]+)['"]?\s+http-equiv=['"]?content-type['"]?''',
    re.I)
META_ENC3_BS_RE = re.compile(
    b'''<meta\s+http-equiv=['"]?charset['"]?\s+content=['"]?([^'" >/]+)''', re.I)
META_ENC4_BS_RE = re.compile(
    b'''<meta\s+content=['"]?([^'" >/]+)['"]?\s+http-equiv=['"]?charset['"]?''', re.I)
META_ENC5_BS_RE = re.compile(b'''<meta\s+charset=['"]?([^'" >/]+)''', re.I)
def html_decode(html_bs, lang):
    #find encoding specified in the header (copied from jusText)
    meta_encoding = None
    for re_meta in (META_ENC1_BS_RE, META_ENC2_BS_RE, META_ENC3_BS_RE, META_ENC4_BS_RE,
            META_ENC5_BS_RE):
        m = re_meta.search(html_bs)
        if m:
            try:
                codec_name = m.group(1).decode('utf-8')
                meta_encoding = codecs.lookup(codec_name).name
            except (UnicodeDecodeError, LookupError, TypeError):
                pass
            else:
                break
    #decode using chared detected encoding
    chared_encoding = None
    chared_encodings = chared_models[lang].classify(html_bs)
    if chared_encodings:
        try:
            chared_encoding = codecs.lookup(chared_encodings[0]).name
        except LookupError:
            pass #None
        else:
            try:
                return html_bs.decode(chared_encoding), meta_encoding, chared_encoding
            except UnicodeError:
                pass #try another encoding
    #decode using html meta encoding
    if meta_encoding and meta_encoding != 'utf-8':
        try:
            return html_bs.decode(meta_encoding), meta_encoding, chared_encoding
        except UnicodeError:
            pass #try another encoding
    #try utf-8 if everything fails
    return html_bs.decode('utf-8'), meta_encoding, chared_encoding #raises UnicodeError

def txt_decode(text_bs, lang):
    #detect encoding using chared
    chared_encodings = chared_models[lang].classify(text_bs)
    for chared_encoding in chared_encodings:
        try:
            return text_bs.decode(chared_encoding), None, chared_encoding
        except UnicodeError:
            pass #try another encoding
    #try utf-8 if everything fails
    return text_bs.decode('utf-8'), None, None #raises UnicodeError

XML_HEADER_RE = re.compile('^\s*<\?xml.*')
def extract_paragraphs(text_bs, file_type, path, lang, binary_file_cache):
    paragraphs, html_root, html_text, page_title = [], None, '', ''
    meta_encoding = chared_encoding = None
    if file_type == 'html':
        #decode and extract paragraphs from HTML data using Justext
        if config.FORCE_ENCODING:
            try:
                html_text = text_bs.decode(config.FORCE_ENCODING)
            except (UnicodeError, ValueError) as e:
                logging.debug('%s decode failed: %s' % (config.FORCE_ENCODING, e))
                return
        else:
            try:
                html_text, meta_encoding, chared_encoding = html_decode(text_bs, lang)
            except (UnicodeError, ValueError) as e:
                logging.debug('html decode failed: %s' % e)
                return
        html_text = html_text.strip()
        if not html_text:
            return
        #check machine translation references in the HTML code
        for mt_string in MT_STRINGS:
            if mt_string in html_text:
                logging.debug('Ignoring MT indicated by "%s"' % mt_string)
                return
        #remove the XML header (XML encoding problems in SAX parser called by Justext)
        html_text = XML_HEADER_RE.sub('', html_text, count=1)
        #get the HTML DOM root element
        try:
            html_root = lxml.html.fromstring(html_text)
        except (LxmlError, ValueError) as e:
            logging.debug('lxml.html.fromstring failed: %s' % e)
            return
        #extract page title
        try:
            page_title = util.clean_xml_val(html_root.find('.//title').text)
            page_title = page_title[:300].strip() #truncate
        except AttributeError:
            page_title = ''
        #preprocess by Justext
        try:
            html_root = justext.preprocess_html_root(html_root)
        except (LxmlError, ValueError) as e:
            logging.debug('justext.preprocess_html_root failed: %s' % e)
            return
        try:
            paragraphs = justext.make_paragraphs(html_root) #stripped
        except (LxmlError, ValueError) as e:
            logging.debug('justext.make_paragraphs failed: %s' % e)
            return
    elif file_type == 'txt':
        #extract paragraphs from a text file
        if config.FORCE_ENCODING:
            txt_data = text_bs.decode(config.FORCE_ENCODING, errors='replace')
        else:
            try:
                txt_data, meta_encoding, chared_encoding = txt_decode(text_bs, lang)
            except (UnicodeError, ValueError) as e:
                logging.debug('txt decode failed: %s' % e)
                return
        if txt_data:
            for par_text in txt_data.split('\n\n'):
                if config.SPACE_SEP_TOKENS:
                    word_count = len(par_text.split())
                else:
                    word_count = 0 #cannot tokenise text, Justext understands the zero count
                paragraphs.append({
                    'text': par_text.strip(),
                    'word_count': word_count,
                    'linked_char_count': 0,
                    'dom_path': 'p',
                })
    elif config.CONVERSION_ENABLED:
        if file_type in CONVERSION_COMMANDS:
            #extract paragraphs from non HTML data using an external converter
            if binary_file_cache:
                txt_data_bs = binary_file_cache[0]
            else:
                txt_data_bs = convert_to_txt(CONVERSION_COMMANDS[file_type], text_bs)
                binary_file_cache.append(txt_data_bs)
            if config.FORCE_ENCODING:
                txt_data = txt_data_bs.decode(config.FORCE_ENCODING, errors='replace')
            else:
                try:
                    txt_data, meta_encoding, chared_encoding = txt_decode(txt_data_bs, lang)
                except (UnicodeError, ValueError) as e:
                    logging.debug('txt decode failed: %s' % e)
                    return
            if txt_data:
                for par_text in txt_data.split('\n\n'):
                    if config.SPACE_SEP_TOKENS:
                        word_count = len(par_text.split())
                    else:
                        word_count = 0 #cannot tokenise text, Justext understands the zero count
                    paragraphs.append({
                        'text': par_text.strip(),
                        'word_count': word_count,
                        'linked_char_count': 0,
                        'dom_path': 'p',
                    })
        else:
            logging.warning('No conversion for %s' % file_type)
    else:
        return #binary file and conversion disabled
    return paragraphs, html_root, html_text, page_title, meta_encoding, chared_encoding

"Extract text from HTML pages using Justext or from other files using a converter."
def any2txt(data_bs, file_type, path, error_msgs):
    #extract paragraphs of plaintext from the input document
    #try for all languages until the difference is below the difference threshold
    min_doc_lang_diff = 1.0
    can_extract_text = False
    best_extracted_data = None
    binary_file_cache = []
    for lang, lang_model in lang_models.items():
        paragraph_data = extract_paragraphs(data_bs, file_type, path, lang, binary_file_cache)
        if paragraph_data and paragraph_data[0]:
            doc_text = '\n'.join([p['text'] for p in paragraph_data[0]]).strip()
            if doc_text:
                doc_lang_diff = get_lang_difference(lang_model, doc_text)
                if doc_lang_diff < min_doc_lang_diff:
                    doc_lang = lang
                    min_doc_lang_diff = doc_lang_diff
                    best_extracted_data = paragraph_data
            can_extract_text = True
    if not can_extract_text:
        error_msgs.append('cannot extract text')
        return
    elif not best_extracted_data:
        error_msgs.append('different from all recognised languages')
        return
    paragraphs, html_root, html_text, page_title, meta_encoding, chared_encoding \
        = best_extracted_data
    #use Justext to classify paragraphs
    justext.classify_paragraphs(
        paragraphs=paragraphs,
        stoplist=justext_wordlists[doc_lang],
        length_low=config.JUSTEXT_LENGTH_LOW,
        length_high=config.JUSTEXT_LENGTH_HIGH,
        stopwords_low=config.JUSTEXT_STOPWORDS_LOW,
        stopwords_high=config.JUSTEXT_STOPWORDS_HIGH,
        max_link_density=config.JUSTEXT_MAX_LINK_DENSITY
    )
    justext.revise_paragraph_classification(
        paragraphs=paragraphs,
        max_good_distance=config.JUSTEXT_MAX_GOOD_DISTANCE,
        max_heading_distance=config.JUSTEXT_MAX_HEADING_DISTANCE
    )
    #return plaintext and prevertical with paragraphs in <p> tags with Justext's classification:
    #context free classes: good/neargood/short/bad, final classes: good/bad
    plaintext, prevertical = [], []
    for p in paragraphs:
        if p['text'] and (config.KEEP_BAD_PARAGRAPHS or p['class'] == 'good'
                or config.ALLOW_NEARGOOD_PARAGRAPHS and p['cfclass'] == 'neargood'):
            p_text = justext.html_escape(p['text'])
            lang_difference = get_lang_difference(lang_models[doc_lang], p_text)
            if lang_difference <= config.LANG_DIFF_THRESHOLD_PAR:
                if p['class'] == 'good' \
                        or config.ALLOW_NEARGOOD_PARAGRAPHS and p['cfclass'] == 'neargood':
                    plaintext.append(p_text)
                heading = ' heading="yes"' if p['heading'] else ''
                justext_classes = ' class="%s" cfclass="%s"' % (p['class'], p['cfclass'])
                prevertical.append('<p%s%s langdiff="%.2f">\n%s\n</p>' %
                    (heading, justext_classes, lang_difference, p_text))
    if plaintext:
        return ('\n'.join(plaintext), '\n'.join(prevertical), html_root, html_text, page_title,
            doc_lang, min_doc_lang_diff, meta_encoding, chared_encoding)
    else:
        error_msgs.append('no good plaintext')

HTML_BASE_TAG_RE = re.compile('<base [^>]*href="([^"]+)/?"', re.UNICODE)
"Extracts links to web pages, links to binary files, blacklisted/unaccepted links"
def extract_links_from_html(html_root, html, src_url, src_host, empty_page):
    result_links, ignored_links, binfile_links = set(), set(), set()
    if html_root is None:
        return (result_links, binfile_links, ignored_links)
    all_links, doc_links = set(), set()
    #check base tag
    m = HTML_BASE_TAG_RE.search(html)
    base_url = '%s/' % m.group(1) if m else src_url
    #extract links, ignore nofollow links
    for a_href in html_root.xpath(".//a[not(@rel='nofollow')]/@href"):
        new_url = ''.join(map(url_strip, a_href.split('\n')))
        if new_url:
            #resolve relative paths, join with base url, normalise (no trailing slash)
            try:
                doc_links.add(url_join_rel_norm(base_url, new_url))
            except ValueError:
                logging.debug('extract_links URL join fail: %s -> %s' % (base_url, new_url))
                continue
    #filter out bad links
    for url in doc_links:
        #omit local duplicates
        if url in all_links:
            continue
        all_links.add(url)
        #split to parts and separate bad URLs
        url_parts, bad_url_reason = url_split_and_check_parts(url, src_host, empty_page)
        if url_parts:
            result_links.add(url_parts) #all checks passed
        elif bad_url_reason == 'path-binary-file':
            binfile_links.add(url)
        else:
            ignored_links.add((bad_url_reason, url))
    return (result_links, binfile_links, ignored_links)

def read_from_downloader(processor_id, processor_pipe_path):
    bytes_read = wpages_read = recently_read_count = redirections_read = postponed_record_count = 0
    recently_added = {} #recently added wpage record count by domain
    arc_bytes_written = arc_number = 0
    MAX_ARC_SIZE = 100*1024**3 #100 GB, uncompressed
    q_wpage_postponed_paths = deque()
    #read raw encoded web pages from files
    with io.open(processor_pipe_path, mode='rt', encoding='utf-8', buffering=util.BUFSIZE,
            newline='\n') as downloader_read_stream, \
            gzip.open('%s/%d.links_redirect.gz' % (IGNORED_DIR, processor_id),
            mode='wt', encoding='utf-8') as ignored_redirects_file:
        while not terminate[0]:
            #try reading a message from the downloader
            wpage_path = None
            ready_for_reading, _, _ = select([downloader_read_stream], [], [], 0)
            if ready_for_reading:
                msg = downloader_read_stream.readline().rstrip() #non-blocking
                #detect redirection
                if msg.startswith(util.MSG_NEW_URL):
                    scheme, host, redir_url = msg.split(' ')[1:]
                    #split to parts and separate bad URLs
                    redir_url_parts, bad_url_reason = url_split_and_check_parts(redir_url, host)
                    if redir_url_parts: #all checks passed
                        redir_scheme, redir_host, redir_path = redir_url_parts
                        q_redirect.append_((scheme, host, redir_scheme, redir_host, redir_path))
                    else:
                        ignored_redirects_file.write('%s %s %s %s\n' %
                            (scheme, host, bad_url_reason, redir_url))
                    redirections_read += 1
                    continue
                #other message => wpage path
                elif msg:
                    wpage_path = msg
                    write_arc = not reprocessing_mode
            if not wpage_path:
                #process postponed records if not overloaded
                if q_web_pages.len_() < 500:
                    try:
                        wpage_path = q_wpage_postponed_paths.popleft()
                    except IndexError:
                        #terminate after reading all input in the reprocessing mode
                        if reprocessing_mode and end_of_input[0]:
                            terminate[0] = True
                            break
                    else:
                        write_arc = False #this postponed record has already been writen
            #read web pages, add them to the queue
            if not wpage_path:
                sleep(30)
                continue
            wpage_batch_count = 0
            #check if the wpage file is compressed, skip if not found
            if not os.path.exists(wpage_path):
                if os.path.exists('%s.gz' % wpage_path):
                    wpage_path = '%s.gz' % wpage_path
                else:
                    logging.warning('file %s not found => skipped' % wpage_path)
                    continue
            #check if overloaded, prevent adding too much records to q_web_pages
            q_web_pages_len = q_web_pages.len_()
            if reprocessing_mode:
                while q_web_pages_len > 200:
                    sleep(30)
                    q_web_pages_len = q_web_pages.len_()
            else:
                while q_web_pages_len > config.MAX_WPAGE_PROCESSING_COUNT and not terminate[0]:
                    logging.debug('wpage Q size delay: %d' % q_web_pages_len)
                    sleep(120)
                    q_web_pages_len = q_web_pages.len_()
                if q_web_pages_len > 9000:
                    max_recent_records_per_domain = 1
                elif q_web_pages_len > 3000:
                    max_recent_records_per_domain = 3
                elif q_web_pages_len > 1500:
                    max_recent_records_per_domain = 10
                elif q_web_pages_len > 500:
                    max_recent_records_per_domain = 30
                else:
                    max_recent_records_per_domain = 100
                web_page_postponed_path = '%s_later.gz' % wpage_path.replace('.gz', '')
                web_page_postponed_fp = gzip.open(web_page_postponed_path, mode='wb')
                postponed_record_count = 0
                #partially reset recently added wpage counts
                recently_added_count = sum(recently_added.values())
                if recently_read_count >= 500000 or recently_added_count >= 80000 \
                        or q_web_pages_len < 500 and recently_added_count:
                    recently_read_count = 0
                    avg = recently_added_count / len(recently_added)
                    recently_added = {k: v - avg for k, v in recently_added.items() if v > avg}
                    logging.debug('recent counters reset: %d --> %d (each domain –%d)' %
                        (recently_added_count, sum(recently_added.values()), avg))
            #read all records from a wpage file
            logging.debug('Reading web pages from %s' % wpage_path)
            open_fn = gzip.open if wpage_path.endswith('.gz') else io.open
            with open_fn(wpage_path, mode='rb') as web_page_file: #encoded
                while True:
                    wpage_header_bs = web_page_file.readline()
                    if not wpage_header_bs:
                        break
                    wpage_id, url, scheme, host, path, ip, connect_time, header_len, \
                        body_len = wpage_header_bs.decode('utf-8').split(' ')
                    header_len, body_len = int(header_len), int(body_len)
                    http_header_bs = web_page_file.read(header_len)
                    http_body_bs = web_page_file.read(body_len)
                    wpage_batch_count += 1
                    bytes_read += header_len + body_len + len(wpage_header_bs)
                    #process only first max_recent_records_per_domain pages if overloaded
                    recent_records_per_this_domain = recently_added.get(host, 0)
                    if not reprocessing_mode \
                            and recent_records_per_this_domain >= max_recent_records_per_domain:
                        #too many pages in this domain recently => process later
                        web_page_postponed_fp.write(wpage_header_bs)
                        web_page_postponed_fp.write(http_header_bs)
                        web_page_postponed_fp.write(http_body_bs)
                        postponed_record_count += 1
                    else:
                        #not too many pages in this domain => add the record to the processing queue
                        q_web_pages.append_((wpage_id, url, scheme, host, path, ip,
                            connect_time, extract_file_type(http_header_bs),
                            extract_last_modified(http_header_bs), http_body_bs))
                        if not reprocessing_mode:
                            recently_added[host] = recent_records_per_this_domain + 1
                    #write the ARC output (UTF-8 header, the rest in the original encoding)
                    #do it for all records the first time they were read
                    if write_arc:
                        arc_header_bs = ('%s %s %s text/html %d %d %s\n' %
                            (url, ip, connect_time, header_len, body_len, wpage_id)).encode('utf-8')
                        with gzip.open('%s/%d-%d.arc.gz' % (ARC_DIR, processor_id, arc_number),
                                mode='ab', compresslevel=9) as arc_file:
                            arc_file.write(arc_header_bs)
                            arc_file.write(http_header_bs)
                            arc_file.write(http_body_bs)
                        arc_bytes_written += header_len + body_len + len(arc_header_bs)
                        if arc_bytes_written >= MAX_ARC_SIZE:
                            arc_number += 1
                            arc_bytes_written = 0
            recently_read_count += wpage_batch_count
            wpages_read += wpage_batch_count
            logging.debug('%d web pages read from %s, %d in the queue now, %d postponed for later' %
                (wpage_batch_count, wpage_path, q_web_pages.len_(), postponed_record_count))
            util.remove_file_if_exists(wpage_path)
            #rename the postponed record file or remove if empty
            if not reprocessing_mode:
                web_page_postponed_fp.close()
                if postponed_record_count:
                    wpage_path = web_page_postponed_path.replace('_later', '')
                    os.rename(web_page_postponed_path, wpage_path)
                    q_wpage_postponed_paths.append(wpage_path)
                else:
                    util.remove_file_if_exists(web_page_postponed_path)
    logging.debug('%d web pages (%d MB) read; %d redirections read; done' %
        (wpages_read, bytes_read // 1024**2, redirections_read))

TOKEN_SEPARATOR_RE = re.compile('\s+', re.UNICODE)
def process_web_pages(processor_id):
    prevert_number = prevert_bytes_written = 0
    prevert_file_batch = []
    PREVERT_BATCH_COUNT = 50 #number of prevert docs written out at once
    MAX_PREVERT_SIZE = 10*1024**3 #10 GB
    with gzip.open('%s/%d.links_binfile.gz' % (IGNORED_DIR, processor_id),
            mode='wt', encoding='utf-8') as binfile_urls_file, \
            gzip.open('%s/%d.links_ignored.gz' % (IGNORED_DIR, processor_id),
            mode='wt', encoding='utf-8') as ignored_urls_file:
        while True:
            try:
                wpage_id, url, scheme, host, path, ip, connect_time, file_type, last_modified, \
                    body_bs = q_web_pages.popleft_()
            except IndexError:
                sleep(20) #web page queue delay
                if terminate[0]:
                    break
                continue
            try:
                #extract clean text, prevertical and document links
                txt = prevert = html = lang = ''
                html_root = meta_enc = chared_enc = None
                error_msgs = []
                extracted_text_and_metadata = any2txt(body_bs, file_type, path, error_msgs)
                if extracted_text_and_metadata:
                    lang, lang_diff = extracted_text_and_metadata[5:7]
                    if lang in config.LANGUAGES_ACCEPT:
                        txt, prevert, html_root, html, page_title, lang, lang_diff, meta_enc, \
                            chared_enc = extracted_text_and_metadata
                    else:
                        error_msgs.append('unaccepted language: %s (%.2f)' % (lang, lang_diff))
                else:
                    error_msgs.append('no text')
                txt_len = len(txt)
                #extract links, deal with links from pages having no text
                links, binfile_links, ignored_links = extract_links_from_html(
                    html_root, html, url, host, empty_page=not txt_len)
                #clean the URL displayed in the prevertical
                url_display = clean_url_display(scheme, host, path)
                #some clean text => store plaintext with metadata in the prevertical format
                if txt_len:
                    prevert_file_batch.append('<doc id="%s" title="%s" length="%s" crawl_date="%s"'
                            '%s lang="%s" lang_diff="%.2f" ip="%s" url="%s" file_type="%s"'
                            ' enc_meta="%s" enc_chared="%s">\n%s\n</doc>\n' %
                        (wpage_id, page_title, format_doc_len(txt_len), format_date(connect_time),
                        ' modified_date="%s"' % last_modified if last_modified else '', lang,
                        lang_diff, ip, url_display, file_type if file_type else '', meta_enc,
                        chared_enc, prevert))
                    token_count = len([x for x in TOKEN_SEPARATOR_RE.split(txt) if x])
                    txt_hash = hash(txt)
                else:
                    token_count = txt_hash = 0
                #add document metadata to the queue, store links
                q_documents.append_((wpage_id, scheme, host, len(html), hash(html), txt_len, lang,
                    txt_hash, token_count, links))
                if binfile_links:
                    binfile_urls_file.write('%s://%s %s\n' % (scheme, host, ' '.join(binfile_links)))
                if ignored_links:
                    ignored_reason_url_pairs = ['%s %s' % x for x in ignored_links]
                    ignored_urls_file.write('%s://%s %s\n' %
                        (scheme, host, ' '.join(ignored_reason_url_pairs)))
                logging.debug('DONE %s://%s%s -- %d chars, %d links, %s' %
                    (scheme, host, path, txt_len, len(links), '; '.join(error_msgs) or 'OK'))
                #write a batch of preverticals
                if len(prevert_file_batch) >= PREVERT_BATCH_COUNT:
                    prevert_file_batch_bs = ''.join(prevert_file_batch).encode('utf-8')
                    prevert_file_batch = []
                    with io.open('%s/%d-%d.prevert_d' % (PREVERT_DIR, processor_id, prevert_number),
                            mode='ab', buffering=util.BUFSIZE) as prevert_file:
                        prevert_file.write(prevert_file_batch_bs)
                    prevert_bytes_written += len(prevert_file_batch_bs)
                if prevert_bytes_written >= MAX_PREVERT_SIZE:
                    prevert_number += 1
                    prevert_bytes_written = 0
            except Exception as e:
                try:
                    import traceback
                    logging.debug('process_web_pages exception: %s\n\tTRACEBACK:\n%s\n' %
                        (e, traceback.format_exc()))
                except:
                    logging.debug('process_web_pages exception')

def write_to_scheduler(processor_id):
    #send redirections and web pages' metadata to the scheduler
    redirects_written = redirect_batch_id = 0
    redirect_batch_size = 1
    redirect_batch_sizes = [config.MAX_REDIR_BATCH_SIZE, 50, 20, 10, 5, 2, 1, 1] #cold start
    docs_written = links_written = metadata_batch_id = 0
    metadata_batch, metadata_batch_len = [], 0
    metadata_batch_size = config.MAX_METADATA_BATCH_SIZE if reprocessing_mode else 1
    metadata_batch_sizes = [config.MAX_METADATA_BATCH_SIZE,
        500, 200, 100, 50, 20, 10, 5, 2, 1, 1] #cold start
    with io.open(sys.stdout.fileno(), mode='wt', encoding='utf-8', buffering=1,
            newline='\n') as scheduler_write_stream:
        while True:
            #redirections
            if q_redirect.len_() >= redirect_batch_size:
                redirect_batch = []
                while True:
                    try:
                        redirect_batch.append(q_redirect.pop_())
                    except IndexError:
                        break #no more redirections
                redirect_batch_id += 1
                redirect_path = '%s/%d-%d' % (REDIRECT_DIR, processor_id, redirect_batch_id)
                with io.open(redirect_path, mode='wt', encoding='utf-8', buffering=util.BUFSIZE) \
                        as redirect_file:
                    redirect_file.write(''.join(
                        '%s\n' % ' '.join(redirect_tuple) for redirect_tuple in redirect_batch))
                scheduler_write_stream.write('%s %s\n' %
                    (util.MSG_NEW_URL, redirect_path)) #line buffered
                redirects_written += len(redirect_batch)
                if redirect_batch_sizes:
                    redirect_batch_size = redirect_batch_sizes.pop()
            #web pages
            try:
                wpage_id, scheme, host, html_len, html_hash, txt_len, lang, txt_hash, token_count, \
                    links = q_documents.popleft_()
            except IndexError:
                sleep(20) #document queue delay
                if not terminate[0]:
                    continue
            else:
                link_count = len(links)
                #send processed documents metadata and extracted URLs/links to the scheduler
                links_s = '\n%s' % '\n'.join([' '.join(x) for x in links]) if links else ''
                doc_metadata = '%s %s %s %d %d %d' % \
                    (wpage_id, scheme, host, html_len, html_hash, txt_len)
                if config.MULTILINGUAL:
                    primary_lang_flag = 'P' if lang in config.PRIMARY_LANGUAGES else '-'
                    doc_metadata = '%s %s' % (doc_metadata, primary_lang_flag)
                doc_metadata = '%s %d %d %d%s\n' % \
                    (doc_metadata, txt_hash, token_count, len(links_s), links_s)
                metadata_batch.append(doc_metadata)
                metadata_batch_len += 1
            if metadata_batch_len and (metadata_batch_len >= metadata_batch_size or terminate[0]):
                metadata_batch_id += 1
                metadata_path = '%s/%d-%d' % (DOC_META_DIR, processor_id, metadata_batch_id)
                with io.open(metadata_path, mode='wt', encoding='utf-8', buffering=util.BUFSIZE) \
                        as metadata_file:
                    metadata_file.write(''.join(metadata_batch))
                scheduler_write_stream.write('%s\n' % metadata_path) #line buffered
                logging.debug('%d documents with %d links written' %
                    (metadata_batch_len, link_count))
                docs_written += metadata_batch_len
                links_written += link_count
                metadata_batch, metadata_batch_len = [], 0
                if not reprocessing_mode and metadata_batch_sizes:
                    metadata_batch_size = metadata_batch_sizes.pop()
            if terminate[0]:
                break
        scheduler_write_stream.write('%s\n' % util.MSG_TERMINATE) #line buffered
    logging.debug('%d documents and %d links written; %d redirections written' %
        (docs_written, links_written, redirects_written))

#thread reading http responses
t_read = Thread(target=read_from_downloader, name='read_from_downloader',
    args=(processor_id, processor_pipe_path))
t_read.start()
#thread processing data into documents
t_process = Thread(target=process_web_pages, name='process_web_pages', args=(processor_id,))
t_process.start()
#thread writing out attributes of processed documents
t_write = Thread(target=write_to_scheduler, name='write_to_scheduler', args=(processor_id,))
t_write.start()
#detect a termination order issued by the scheduler through stdin
#normal processing mode => process the current batch of data and terminate
#reprocessing mode => wait until all data is processed, then terminate
while True:
    if sys.stdin.readline().startswith(util.MSG_TERMINATE): #blocks until read
        if reprocessing_mode:
            logging.info('Processor #%d will terminate after processig all input' % processor_id)
            end_of_input[0] = True
        else:
            logging.info('Processor #%d will terminate after processig the current batch of data' %
                processor_id)
            terminate[0] = True
        break
    sleep(30)
#wait for threads to finish
t_read.join()
t_process.join()
t_write.join()
logging.info('Processor %d finished' % processor_id)
