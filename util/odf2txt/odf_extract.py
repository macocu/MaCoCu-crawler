#!/usr/bin/python3

import sys

from odf import text, teletype
from odf.opendocument import load

doc = load(open(sys.stdin.fileno(), 'rb'))

paragraphs_txt = []
for paragraph in doc.getElementsByType(text.P):
    paragraph_txt = teletype.extractText(paragraph).strip()
    if paragraph_txt:
        paragraphs_txt.append(paragraph_txt)

sys.stdout.write('\n\n'.join(paragraphs_txt))

