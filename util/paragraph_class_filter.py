#!/usr/bin/env pypy3

import sys, re

good_par_re = re.compile(' class="good"')

i = 0
in_par = False
good_par = False
par_lines = []
for line in sys.stdin:
    if line.startswith('<p '):
        if good_par and len(par_lines) > 1:
            par_lines.append('</p>\n')
            sys.stdout.write(''.join(par_lines))
        in_par = True
        good_par = bool(good_par_re.search(line))
        par_lines = [line]
    elif line.startswith('</p>'):
        if good_par and len(par_lines) > 1:
            par_lines.append(line)
            sys.stdout.write(''.join(par_lines))
        in_par = False
        par_lines = []
    elif in_par:
        par_lines.append(line)
    else:
        sys.stdout.write(line)
    i += 1
    if i % 1000000 == 0:
        sys.stderr.write('\r%d lines read' % i)
sys.stderr.write('\r%d lines read\n' % i)

