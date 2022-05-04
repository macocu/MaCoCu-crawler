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
Transforms the input containing space separated columns url, scheme, host, path, ip
to columns scheme, host, path, ip, robot_flag. The robot flag is always empty.
"""

import sys

#optional limits passed as arguments
MAX_HOSTNAME_LENGTH = None
MAX_HOST_PATHS = None
rejected_fp = None
for arg in sys.argv[1:]:
    if '--max-hostname-length=' in arg:
        MAX_HOSTNAME_LENGTH = int(arg.split('=', 1)[1])
    elif '--max-host-paths=' in arg:
        MAX_HOST_PATHS = int(arg.split('=', 1)[1])
    elif '--rejected-file=' in arg:
        rejected_fp = open(arg.split('=', 1)[1], 'wt')

NOROBOT_FLAG = ''

rejected_hostname = rejected_host_paths = 0
host_path_counts = {}
for line in sys.stdin:
    #read an input line
    line = line.rstrip()
    try:
        url, scheme, host, path, ip = line.split(' ')
    except ValueError:
        sys.stderr.write('Ignoring invalid line "%s"\n' % line)
        continue
    #checks
    if MAX_HOSTNAME_LENGTH and len(host) > MAX_HOSTNAME_LENGTH:
        if rejected_fp:
            rejected_fp.write('%s\thost name length\n' % host)
        rejected_hostname += 1
        continue
    host_path_count = host_path_counts.get(host, 0)
    if MAX_HOST_PATHS and host_path_count >= MAX_HOST_PATHS:
        if rejected_fp:
            rejected_fp.write('%s\thost path count\n' % host)
        rejected_host_paths += 1
        continue
    host_path_counts[host] = host_path_count + 1
    #re-format columns
    sys.stdout.write('%s\n' % ' '.join((scheme, host, path, ip, NOROBOT_FLAG)))

if rejected_fp:
    rejected_fp.close()
if MAX_HOSTNAME_LENGTH:
    sys.stderr.write('%d records rejected because of a long hostname\n' % rejected_hostname)
if MAX_HOST_PATHS:
    sys.stderr.write('%d records rejected because of the host path limit\n' % rejected_host_paths)
