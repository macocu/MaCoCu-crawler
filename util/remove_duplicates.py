#!/usr/bin/env pypy3

"""Removes duplicates from a prevertical using a duplicate ids file."""

import re, sys

if 2 > len(sys.argv) or '-h' in sys.argv or '--help' in sys.argv:
    sys.stderr.write('Usage: %s [-v] DUPLICATES_FILE < PREVERT_FILE\n' % sys.argv[0])
    sys.exit()
verbose = '-v' in sys.argv or '--verbose' in sys.argv
dup_file = sys.argv[1]

if verbose: sys.stderr.write('Reading duplicate ids...')
with open(dup_file, mode='rt', encoding='utf-8', newline='\n') as duplicates_file:
    duplicates = set([int(x) for x in duplicates_file.read().split('\n') if x])
if verbose: sys.stderr.write('%d read\n' % len(duplicates))

def read_big_structures(fp, structure_tag=b'doc', buffer_size=16777216):
    structure_start_re = re.compile(b'^<%s[ >]' % structure_tag, re.M)
    structure_end = b'</%s>' % structure_tag
    buffer_ = b''
    while True:
        new_data = fp.read(buffer_size)
        if not new_data:
            break
        buffer_ += new_data
        starting_positions = [m.start() for m in structure_start_re.finditer(buffer_)]
        if starting_positions == []:
            continue
        for i in range(len(starting_positions) - 1):
            start = starting_positions[i]
            end = starting_positions[i + 1]
            structure_data = buffer_[start:end]
            if structure_data.rstrip().endswith(structure_end): #check the structure is complete
                yield structure_data
            else:
                yield b''
        buffer_ = buffer_[starting_positions[-1]:]
    if buffer_ != b'':
        if buffer_.rstrip().endswith(structure_end): #check the structure is complete
            yield buffer_
        else:
            yield b''

if verbose: sys.stderr.write('Filtering documents...\n')
stdin = open(sys.stdin.fileno(), 'rb')
stdout = open(sys.stdout.fileno(), 'wb')
doc_all = doc_kept = 0
doc_id_re = re.compile(b'<doc id="([^"]+)"')
for doc in read_big_structures(stdin):
    doc_all += 1
    if verbose and doc_all % 10**6 == 0:
        sys.stderr.write('Doc %d\n' % doc_all)
    if not doc:
        sys.stderr.write('Skipping invalid doc %d\n' % doc_all)
        continue
    header = doc.split(b'\n', 1)[0]
    try:
        doc_id = int(re.match(doc_id_re, header).group(1))
    except ValueError:
        sys.stderr.write('Invalid document header: "%s"\n' % header)
        raise
    if doc_id not in duplicates:
        doc_kept += 1
        stdout.write(doc)
if verbose: sys.stderr.write('%d/%d documents written\n' % (doc_kept, doc_all))
