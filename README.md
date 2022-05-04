# MaCoCu crawler – A web TLD spider/crawler

## Requires
- python >= 3.6,
- pypy3 >= 5.5 (optional),
- justext >= 3.0 (http://corpus.tools/wiki/Justext),
- chared >= 2.0 (http://corpus.tools/wiki/Chared),
- lxml >= 4.2 (http://lxml.de/),
- openssl >= 1.1,
- pyre2 >= 0.2.23 (https://github.com/andreasvc/pyre2),
- text processing tools (if binary format conversion is on):
  - pdftotext (from poppler-utils),
  - ps2ascii (from ghostscript-core),
  - antiword (from antiword + perl-Time-HiRes),
  - odfpy
- nice (coreutils) (optional),
- ionice (util-linux) (optional),
- gzip (optional).

Runs in Linux -- Fedora/RHEL, there are also users who run it in Ubuntu.

Minimum hardware configuration (very small crawls):
- 4 core CPU,
- 8 GB system memory,
- some storage space,
- broadband internet connection.

Recommended hardware configuration (crawling ~30 bn words of English text):
- 8-32 core CPU (the more CPUs the faster the processing of crawled data),
- 32-256 GB system memory
    (the more RAM the more domains kept in memory and thus more webs visited),
- lots of storage space,
- connection to an internet backbone line.

## Includes
A robot exclusion rules parser for Python (v. 1.6.2)
- by Philip Semanchuk, BSD Licence
- see util/robotparser.py

Language detection using character trigrams
- by Douglas Bagnall, Python Software Foundation Licence
- see util/trigrams.py

docx2txt
- by Sandeep Kumar, GNU GPL 3+
- see util/doc2txt.pl

## Installation
- unpack: ```tar -xJvf macocu_crawler-src-*.tar.xz```
- install required tools, see install_rpm.sh for rpm based systems
- check importing the following dependences by pypy3/python3:
  ```
  python3 -c 'import justext.core, chared.detector, ssl, lxml, re2'
  pypy3 -c 'import ssl; from ssl import PROTOCOL_TLS'
  ```
- make sure the crawler can write to config.RUN_DIR and config.PIPE_DIR.


### Settings -- edit util/config.py
- !!!IMPORTANT!!! Set AGENT, AGENT_URL, USER_AGENT;
- set MAX_RUN_TIME to specify max crawling time in seconds;
- set DOC_PROCESSOR_COUNT to (partially) control CPU usage;
- set MAX_OPEN_CONNS, IP_CONN_INTERVAL, HOST_CONN_INTERVAL;
- raise ulimit -n accoring to MAX_OPEN_CONNS;
- then increase MAX_OPEN_CONNS and OPEN_AT_ONCE;
- configure language and TLD dependent settings.

### Language models for all recognised languages
- Plaintext in util/lang_samples/,
    e.g. put plaintexts from several dozens of nice web documents and Wikipedia
    articels, manually checked, in util/lang_samples/{Czech,Slovak,English};
- jusText wordlists in util/justext_wordlists/,
    e.g. use the Justext default or 2000 most frequent manually cleaned words,
    one per line, in util/justext_wordlists/{Czech,Slovak,English};
- chared model in util/chared_models/,
    e.g. copy the default chared models {czech,slovak,english}.edm to
    util/chared_models/{Czech,Slovak,English}
See default English resources in the respective directories.

## Usage
./scheduler.py -h prints the full usage.
It is recommended to run the crawler in `screen`.
Example:
```
screen -S crawling
./scheduler.py < seed_urls &> run/out &
```

### Files created by the crawler in run/:
- *.log.* .. log & debug files,
- arc/*.arc.gz .. gzipped arc files (raw http responses),
- prevert/*.prevert_d .. preverticals with duplicate documents,
- prevert/duplicate_ids .. files duplicate document IDs,
- ignored/* .. ignored URLs (binary files (pdf, ps, doc, docx) which were
    not processed and urls not passing the domain blacklist or the TLD filter),
- save/* .. savepoints that can be used for a new run,
- other directories can be erased after stopping the crawler.

### To remove duplicate documents from preverticals, run
```
for pdup in run/prevert/*.prevert_d
do
    p=`echo $pdup | sed -r 's,prevert_d$,prevert,'`
    pypy3 util/remove_duplicates.py run/prevert/duplicate_ids < $pdup > p
done
```
Files run/prevert/*.prevert are the final output.
Onion (http://corpus.tools/wiki/Onion) is recommended to remove near-duplicate
paragraphs of text.

### To stop the crawler before MAX_RUN_TIME, send SIGTERM to the main process
(pypy/python scheduler.py).
Example:
```
ps aux | grep 'scheduler\.py' #find the PID of the main process
kill -s SIGTERM PID
```

### To re-process arc files with current process.py and util/config.py, run
```
for arcf in run/arc/*.arc.gz
do
    p=`echo $arcf | sed -r 's,run/arc/([0-9]+)\.arc\.gz$,\1.prevert_re_d,'`
    zcat $arcf | pypy3 reprocess.py > $p
done
```

### To re-start crawling from the last saved state:
```
#rename the old `run' directory and make a new one for the new run
mv -iv run old
mkdir run run/url
#make the new run continue from the previous state by symlinking old data
for d in docmeta dompath domrobot domsleep robots; do ln -s ../old/$d/ run/$d; done
cp old/url/dns_table old/url/urls_waiting run/url/
#list paths to unprocessed wpage files to process in the new run
ls old/wpage/* | sort -t '/' -k3,3g > old/wpage_paths
#run the crawler again
screen -r crawling
./scheduler.py \
    --state-files=old/save/domains_T.gz,old/save/raw_hashes_T.gz,old/save/txt_hashes_T.gz \
    --wpage-paths-file=old/wpage_paths \
    --old-tuples < old/url/download_queue &> run/out &
#assuming T is the timestamp of the last save, e.g. 20190616231500
```

### Performance tips
- Start with thousands of seed URLs. Give more URLs per domain.
  It is possible to start with tens of millions of seed URLs.
  (If you need to start with a small number of seed URLs, set
  VERY_SMALL_START = True in util/config.py.)
- Start with URLs of trustworthy sites providing a lot of text, e.g. sites of
  the government, newspapers, magazines, universities, big companies, famous
  personal blogs, well known topical blogs, culture providers, churches.
- Using PyPy reduces CPU cost and may cost more RAM.
- Set TLD_WHITELIST to avoid crawling domains not in the target language
  (it takes some resources to detect it otherwise).
- Set LOG_LEVEL to debug and set INFO_PERIOD to check the debug output in
  *.log.crawl and *.log.eval to see where the bottleneck is, modify the
  settings accordingly, e.g. add doc processors if the doc queue is always full.

## Acknowledgements
The author would like to express many thanks to Jan Pomikálek, Pavel Rychlý
and Miloš Jakubíček for guidance, key design advice and help with debugging.
Thanks also to Vlado Benko and Nikola Ljubešić for ideas for improvement.

Based on crawler SpiderLing which was supported by Lexical Computing
(https://www.lexicalcomputing.com/) and Natural Language Processing Centre
of Masaryk University (https://nlp.fi.muni.cz/en/NLPCentre).
