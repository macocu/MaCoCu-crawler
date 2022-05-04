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

#===============================================================================
# SPIDERLING CONFIGURATION FILE
# Set the most important configuration options here, e.g. USER_AGENT
# Sections: General, Web downloader, Domains, Data processing, Logging
#===============================================================================


#== General ==

#File directories
RUN_DIR = 'run'
LOG_DIR = RUN_DIR
PIPE_DIR = '%s/pipes' % RUN_DIR #interprocess communication
URL_DIR = '%s/url' % RUN_DIR #URLs to download
ROBOTS_DIR = '%s/robots' % RUN_DIR #robots.txt
REDIRECT_DIR = '%s/redirect' % RUN_DIR #HTTP redirections
WPAGE_DIR = '%s/wpage' % RUN_DIR #raw encoded downloaded documents
ARC_DIR = '%s/arc' % RUN_DIR #raw encoded downloaded documents in the arc format
DOC_META_DIR = '%s/docmeta' % RUN_DIR #document metadata and extracted URLs
PREVERT_DIR = '%s/prevert' % RUN_DIR #processed documents in the prevertical format
IGNORED_DIR = '%s/ignored' % RUN_DIR #URLs excluded from further processing
DOM_SLEEP_DIR = '%s/domsleep' % RUN_DIR #sleeping domains (not needed in RAM)
DOM_PATH_DIR = '%s/dompath' % RUN_DIR #new paths in domains
DOM_ROBOT_DIR = '%s/domrobot' % RUN_DIR #domain robots
SAVE_DIR = '%s/save' % RUN_DIR #saved states

#Max crawling time [s] (None for no limit)
MAX_RUN_TIME = None

#A simple switch to adjust data amount related settings.
#Switch on when crawling a big language and starting with > 1M seed URLs
#and expecting a lot of data and can afford losing some data due to a stricter
#HTML cleaning or due to controlling domain scheduling priority.
BIG_CRAWLING = False
#A simple switch to adjust CPU and memory related settings.
#Switch on if having a lot of resources (CPUs, RAM) and ulimit -n > 10000.
BIG_MACHINE = False
#A simple switch to adjust Interprocess communication file size thresholds.
#Switch on if starting with < 1000 seed URLs.
MINIMAL_SEEDS = False

#Set MULTILINGUAL to True when multiple languages are accepted but the presence of some of them
#is required in the crawled texts, e.g. when crawling Norwegian TLD and interested in monolingual
#Norwegian sites as well as bilingual English/Norwegian sites. The required languages should be
#defined in PRIMARY_LANGUAGES.
MULTILINGUAL = False

#The number of downloaded data processing threads (process.py) to run,
#2 is enough for very small languages, 16 is enough for huge English crawls.
#Do not set to more than n - 2 where n = number of your processor cores.
#A single document processor can deal with approx. 1000 MB/hour raw data
#download speed (a rough estimate, depends on CPU).
if BIG_CRAWLING and BIG_MACHINE:
    DOC_PROCESSOR_COUNT = 16
elif BIG_MACHINE:
    DOC_PROCESSOR_COUNT = 8
else:
    DOC_PROCESSOR_COUNT = 4

#Interpreter and priority of subprocesses spawned by the main script
DOWNLOADER_EXEC = ['pypy3', 'crawl.py'] #pypy3 or python3 possible
PROCESSOR_EXEC = ['ionice', '-c3', 'nice', '-n10', 'python3', 'process.py'] #python3 only

#Interprocess communication – number of URLs/web pages
if MINIMAL_SEEDS:
    MIN_URL_SELECT_START = 1
    MIN_URL_SELECT = 10 #minimum 10
    MAX_REDIR_BATCH_SIZE = 50 #minimum 50
    MAX_METADATA_BATCH_SIZE = 500 #minimum 500
    MAX_WPAGE_BATCH_SIZE = 200 #minimum 200
    MIN_NEW_URL_BATCH_SIZE = 100 #minimum 100
else:
    MIN_URL_SELECT_START = 100 if BIG_CRAWLING else 1
    MIN_URL_SELECT = 3*1000 if BIG_CRAWLING else 300
    MAX_REDIR_BATCH_SIZE = 5*1000 if BIG_CRAWLING else 100
    MAX_METADATA_BATCH_SIZE = 5*1000 if BIG_CRAWLING else 1000
    MAX_WPAGE_BATCH_SIZE = 10*1000 if BIG_CRAWLING else 1000
    MIN_NEW_URL_BATCH_SIZE = 5*1000 if BIG_CRAWLING else 250
MAX_URL_SELECT = 100*1000 if BIG_CRAWLING else 30*1000 #minimum 100
MAX_WRITE_TO_SCHEDULER_DELAY = 3*60 if BIG_CRAWLING else 60 #[s]
MAX_WPAGE_PROCESSING_COUNT = 20*1000 if BIG_MACHINE else 10*1000


#== Web downloader (mostly related to crawl.py) ==

#You are responsible for properly identifying your crawler.
#Some sites use the '+' in AGENT_URL to reject bots => it is polite to keep it.
#The crawler's behaviour and possible masking is __your__ responsibility.
AGENT = '' #crawler's identity, CHANGE e.g. to your project name
AGENT_URL = '+https://example.com/crawler.html' #CHANGE to your project URL
USER_AGENT = '%s (%s)' % (AGENT, AGENT_URL) #used in HTTP requests

#HTTPS support, fall back to http if disabled
#Used ssl.SSLContext functions require Python >= 2.7.9.
#Disabling results in ignoring webs requiring SSL.
SSL_ENABLED = True

MAX_HOSTNAME_LEN = 50 #long host names are more likely to contain spam

#HTTP response size constraints
DOC_SIZE_MIN = 200
DOC_SIZE_MAX = 10*1024*1024 #do not set to less than 10000

#Max number of open connections (max: ulimit -n), must be >> OPEN_AT_ONCE,
#so do not forget raising ulimit to e.g. 32000 if you have a BIG_MACHINE.
MAX_OPEN_CONNS = 30000 if BIG_CRAWLING and BIG_MACHINE \
    else 10000 if BIG_MACHINE else 4000
#Max number of connections opened at once
OPEN_AT_ONCE = 2000 if BIG_CRAWLING and BIG_MACHINE \
    else 1000 if BIG_MACHINE else 200
#Abandon open connections when not responding after time [s]
NO_ACTIVITY_TIMEOUT = 80 if BIG_CRAWLING and BIG_MACHINE else 40

#Max number of URLs waiting to be downloaded, decrease to save RAM and
#speed up URL selection for download at the cost of good host distribution;
#up to MAX_URL_QUEUE / MAX_HOST_URL_QUEUE hosts can fit in the queue.
MAX_URL_QUEUE = 3*1000*1000 if BIG_MACHINE and BIG_CRAWLING else 1*1000*1000
#Max number of URLs in the download queue after it is updated from
#url/urls_waiting -- ensure MAX_URL_QUEUE_UPDATED > MAX_URL_QUEUE to allow
#updating the queue from the file even after it reached MAX_URL_QUEUE items.
MAX_URL_QUEUE_UPDATED = int(MAX_URL_QUEUE * 1.2)
#Max number of documents waiting to be sent
MAX_WPAGE_QUEUE = 20*1000 if BIG_CRAWLING else 5*1000
#Period of updating the download queue from the waiting queue [s]
#The time to read and write url/urls_waiting (up to 50 GB if BIG_CRAWLING)
#must be less than this value
UPDATE_WAITING_QUEUE_PERIOD = 3600
#Size of download queue wait file to read in memory
#(Every 100 MB takes approx. 750 MB RAM in CPython and 1.5 GB RAM in PyPy)
URL_WAITING_CHUNK_SIZE = 512*1024*1024 if BIG_MACHINE else 64*1024*1024
#Max number of URLs per host in the download queue, decrease to improve host
#distribution. Oversize URLs are written to url/urls_waiting. Ideally,
#MAX_HOST_URL_QUEUE * HOST_CONN_INTERVAL ≅ UPDATE_WAITING_QUEUE_PERIOD + 300.
MAX_HOST_URL_QUEUE = 200

#Period [s] of connecting to the same IP
IP_CONN_INTERVAL = 1
#Period [s] of connecting to the same host =~ crawl delay (20 means 3/min)
HOST_CONN_INTERVAL = 20
#The crawler's behaviour is __your__ responsibility,
#setting these values too low will result in banning your IP by target sites
#and can lead to blocking your internet connection by your ISP!
#crawl.MAX_HOST_URL_QUEUE * HOST_CONN_INTERVAL < UPDATE_WAITING_QUEUE_PERIOD

#Hostname -> IP mapping file (two space separated columns), None by default
#Use recent DNS mappings only since hosts tend to move occasionally.
DNS_TABLE_PATH = None

#Allowed top level domains regexp – crawling is restricted to these, e.g.:
#English + general web: '\.(?:uk|us|au|ca|com|org|net|info|biz|edu|name)$'
#German + general web: '\.(?:de|at|ch|com|org|net|info|eu|edu|name)$'
#Czech only: '\.cz$'
#no restriction (all): ''
TLD_WHITELIST = '' #regexp
#Country/language native TLDs, e.g. '\.(?:uk|us|au|ca)$' or '\.cz$' or ''.
#If EXTRACT_EMPTY_PAGE_EXTERNAL_LINKS == True, a matched cross domain link
#will be followed. '' is the default and results in following all links.
TLD_NATIVE = '' #regexp
#Use e.g. '\.dk$' to block the Danish TLD,
#TLD blacklist > TLD whitelist, '' by default.
TLD_BLACKLIST = '\.tk$' #regexp #Tokelauan domains tend to contain garbage
#National TLDs for the reference:
#ac|ad|ae|af|ag|ai|al|am|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|
#bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cu|cv|cw|
#cx|cy|cz|de|dj|dk|dm|do|dz|ec|ee|eg|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|
#gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|
#io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|
#ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|
#mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|
#pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sk|sl|sm|sn|so|sr|st|su|sv|
#sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|
#vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|za|zm|zw|рф|бел|укр|срб|бг|қаз|мкд|ελ

#web domain lists compiled into a regular expression
#file containing unwanted web domains (one per line), None by default
DOMAIN_BLACKLIST_PATH = None
#same like DOMAIN_BLACKLIST_PATH but an exact match list, no RE
DOMAIN_BLACKLIST_EXACT_PATH = None
#file containing allowed web domains (one per line), None by default
#priority: blacklist > whitelist > allowed TLDs
DOMAIN_WHITELIST_PATH = None

#extract links from web pages containing no clean text
#useful to decrease crawling from domains in unwanted languages
#which is dealt with by the trigram model
#disabling this may increase crawling efficiency (yield rate)
#at the cost of omitting some web domains (or visiting them later)
EXTRACT_EMPTY_PAGE_INTERNAL_LINKS = True
#extract links from pages with no clean text going out from the domain
#priority: EXTRACT_EMPTY_PAGE_LINKS > EXTRACT_EMPTY_PAGE_EXTERNAL_LINKS
EXTRACT_EMPTY_PAGE_EXTERNAL_LINKS = False


#== Domains (mostly related to scheduler.py and util/domains.py) ==

#Min docs downloaded from a domain before yield rate threshold applies
MIN_DOCS_DOWNLOADED = 50 if BIG_MACHINE else 20
#Min bytes downloaded from a domain before yield rate threshold applies
MIN_BYTES_DOWNLOADED = 512*1024 if BIG_MACHINE else 256*1024
#Max (soft limit) cleaned non-empty docs from a domain (None for no limit)
MAX_DOCS_CLEANED = None
#Max domain distance from domains in seed URLs
MAX_DOMAIN_DISTANCE = 50

#Domain scheduling priority scaling by hostname length
if BIG_CRAWLING:
    DOM_SCHED_HOSTNAME_LEN_RANGES = [15, 30, MAX_HOSTNAME_LEN]
else:
    DOM_SCHED_HOSTNAME_LEN_RANGES = [MAX_HOSTNAME_LEN] #disabled
#Domain scheduling priority scaling by domain distance from seeds
if BIG_CRAWLING:
    DOM_SCHED_DOM_DISTANCE_RANGES = [3, 5, 8, 15, MAX_DOMAIN_DISTANCE]
else:
    DOM_SCHED_DOM_DISTANCE_RANGES = [MAX_DOMAIN_DISTANCE] #disabled


#A domain is no longer efficient once it's text yield rate drops below the following threshold
#defined as a function of the count of documents (pages) downloaded from the domain
from math import log
if BIG_CRAWLING:
    def yield_rate_threshold(doc_count):
        #strict: 100 documents: 1 %, 1000: 2 %, 10k: 3 %, 100k: 4 %
        #return 0.01 * (log(doc_count, 10) - 1)
        #normal: 100: 0.5 %, 1000: 1.0 %, 10k: 1.5 %, 100k: 2.0 %
        return 0.005 * (log(doc_count, 10) - 1)
else:
    def yield_rate_threshold(doc_count):
        #permissive: 100: 0.2 %, 1000: 0.4 %, 10k: 0.6 %, 100k: 0.8 %
        return 0.002 * (log(doc_count, 10) - 1)
        #threshold disabled
        #return 0.0

#A domain is no longer efficient once it's ratio of primary language text to all text drops below
#the following threshold defined as a function of the count of documents downloaded from the domain
if MULTILINGUAL:
    def primary_lang_ratio_threshold(doc_count):
        return 0.05 * (log(doc_count, 10) - 1)

#Time [s] after which an unchanged domain (e.g. no data received) is moved
#from RAM to a file.
MAX_IDLE_TIME = 8*3600 if BIG_CRAWLING else 3600
#Period [s] of domain refresh (adding new, removing old, evaluating efficiency)
if MINIMAL_SEEDS:
    UPDATE_DOMAINS_PERIOD = 400
else:
    UPDATE_DOMAINS_PERIOD = 1800 if BIG_CRAWLING else 1200

#Decrease to improve domain variety and thus crawling speed, useful only when
#there is more than ~10000 domains crawled concurrently (big languages)
MAX_URL_SELECT_PER_DOMAIN = 5 if BIG_CRAWLING else 20

#Domain new path count limit to decrease memory consumption. The new paths over
#MAX_DOMAIN_NEW_PATHS will be written to DOM_PATH_DIR and retrieved if the count
#drops below MIN_DOMAIN_NEW_PATHS. Decrease the values to save the RAM.
#Make sure MIN_DOMAIN_NEW_PATHS >= MAX_URL_SELECT_PER_DOMAIN.
MAX_DOMAIN_NEW_PATHS = 250 if BIG_CRAWLING and BIG_MACHINE else 100
MIN_DOMAIN_NEW_PATHS = MAX_URL_SELECT_PER_DOMAIN

#Ignore the robots protocol if the crawler failed to fetch or parse robots.txt
IGNORE_ROBOTS_WHEN_FAILED = False
#Give up waiting for robots.txt for a domain in time [s] (None to turn off),
#apply the ignore decision (see above)
MAX_ROBOT_WAITING_TIME = 3*3600 if BIG_CRAWLING else 1*3600

#Enable to write decoded IDNA hostnames in document URL in prevertical,
#e.g. "https://xn--j1ay.xn--p1ai/" can be decoded to "https://кц.рф/".
DECODE_IDNA_HOSTNAMES = True

#== Data processing (mostly related to processor.py) ==

#Set the languages of documents to recognise, e.g. {'Norwegian', 'Danish'}.
#This enables recognising unwanted languages similar to target languages.
#Requires
# - a plaintext in that languages: ./util/lang_samples/Norwegian
# - a jusText wordlist for that languages: ./util/justext_wordlists/Norwegian
# - a chared model for that languages: ./util/chared_models/Norwegian
#The most similar language is selected (if below lang diff thresholds).
LANGUAGES = {'Norwegian', 'English'}
#Set which of recognised languages to accept, e.g. {'Norwegian'}.
LANGUAGES_ACCEPT = {'Norwegian'}
#Set which of recognised languages are primary, e.g. {'Norwegian'} -- see MULTILINGUAL above.
if MULTILINGUAL:
    PRIMARY_LANGUAGES = {'Norwegian'}

#Max allowed difference to the lang model for document and paragraphs
#(0.0 = same as the model); it does not work well with paragraphs, the reported
#Similarity is usually low
LANG_DIFF_THRESHOLD_DOC = 0.6
LANG_DIFF_THRESHOLD_PAR = 0.95
#Force an encoding (e.g. 'utf-8'), None by default (=> encoding detection)
FORCE_ENCODING = None
#Switch to unigram models (useful for Chinese, Japanese, Korean)
UNIGRAM_MODELS = False
#Space separable tokens – internal tokenisation and justext config switch;
#set to False for scripts not separating words by space (e.g. Chinese)
SPACE_SEP_TOKENS = True

#Justext paragraph heuristic configuration
#Character count < length_low => bad or short
JUSTEXT_LENGTH_LOW = 70 if BIG_CRAWLING else 50 #default = 70
#Character count > length_high => good
JUSTEXT_LENGTH_HIGH = 140 if BIG_CRAWLING else 100 #default = 200
#Number of words frequent in the language >= stopwords_low => neargood
JUSTEXT_STOPWORDS_LOW = 0.2 if SPACE_SEP_TOKENS else 0.0 #default = 0.3
#Number of words frequent in the language >= stopwords_high => good or neargood
JUSTEXT_STOPWORDS_HIGH = 0.3 if SPACE_SEP_TOKENS else 0.0 #default = 0.32
#Density of link words (words inside the <a> tag) > max_link_density => bad
JUSTEXT_MAX_LINK_DENSITY = 0.4 #default = 0.2
#Short paragraph block within the distance of # pars from a good par => good
JUSTEXT_MAX_GOOD_DISTANCE = 5 #default = 5
#Short/near-good headings in the distance of # chars before a good par => good
JUSTEXT_MAX_HEADING_DISTANCE = 150 #default = 200

#Set KEEP_BAD_PARAGRAPHS to keep even short or bad paragraphs (as marked by Justext) in the
#prevertical, the paragraph classification attributes can be used to extract good paragraphs later.
#Note that bad paragraphs are not accounted in the plaintext size even if this flag is on.
KEEP_BAD_PARAGRAPHS = False
#Set ALLOW_NEARGOOD_PARAGRAPHS to keep neargood paragraphs (as marked by Justext) in the
#prevertical (overrides KEEP_BAD_PARAGRAPHS = False) and to account them in the plaintext size.
ALLOW_NEARGOOD_PARAGRAPHS = not BIG_CRAWLING

#Path to justext wordlists, use None for the justext default
#JUSTEXT_WORDLIST_DIR/LANGUAGE must exist for all LANGUAGES
JUSTEXT_WORDLIST_DIR = 'util/justext_wordlists' if SPACE_SEP_TOKENS else None
#Path to chared models
#CHARED_MODEL_DIR/LANGUAGE must exist for all LANGUAGES
CHARED_MODEL_DIR = 'util/chared_models'
#Allow conversion of binary formats (pdf, doc) to text
#Not tested much, the conversion is unreliable, may result in garbage output.
#Main issues: No text layer, hyphenation, page headers/footers, format change.
CONVERSION_ENABLED = False


#== Logging ==

import logging
#general logging settings
LOG_LEVEL = logging.INFO #DEBUG|INFO|WARNING|ERROR
LOG_FORMAT = '%(asctime)-15s %(threadName)s %(message)s'
#Period [s] of writing out some debug info, 0 to disable
INFO_PERIOD = 3600
#Period [s] of logging memory sizes of data structures (debug), 0 to disable
#For debugging, works with Python3/CPython only, keep disabled with PyPy
SIZES_PERIOD = 0
#Period [s] of saving state of the crawler (all domain data), 0 to disable
SAVE_PERIOD = 3600*24
#Log stream buffering (0 = none, 1 = line, -1 = fully/system, n = n bytes)
LOG_BUFFERING = 1
