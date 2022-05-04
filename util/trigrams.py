"""
Created by Douglas Bagnall
Licensed under the PSF License
Obtained from http://code.activestate.com/recipes/326576-language-detection-using-character-trigrams/
Unnecessary code removed by VS
"""

import re

space_re = re.compile('\s+')

class Trigram:
    """From one or more text files, the frequency of three character
    sequences is calculated.  When treated as a vector, this information
    can be compared to other trigrams, and the difference between them
    seen as an angle.  The cosine of this angle varies between 1 for
    complete similarity, and 0 for utter difference.  Since letter
    combinations are characteristic to a language, this can be used to
    determine the language of a body of text. For example:

        >>> reference_en = Trigram('/path/to/reference/text/english')
        >>> reference_de = Trigram('/path/to/reference/text/german')
        >>> unknown = Trigram('url://pointing/to/unknown/text')
        >>> unknown.similarity(reference_de)
        0.4
        >>> unknown.similarity(reference_en)
        0.95

    would indicate the unknown text is almost cetrtainly English. As
    syntax sugar, the minus sign is overloaded to return the difference
    between texts, so the above objects would give you:

        >>> unknown - reference_de
        0.6
        >>> reference_en - unknown    # order doesn't matter.
        0.05
    """
    length = 0

    def __init__(self):
        self.lut = {}

    def parseLines(self, lines):
        if isinstance(lines, (str, bytes)):
            raise ValueError('Lines cannot be a str or bytes.')
        pair = '  '
        for line in lines:
            line = space_re.sub(' ', line)
            if line.strip() == '':
                continue
            for letter in line.strip() + ' ':
                d = self.lut.setdefault(pair, {})
                d[letter] = d.get(letter, 0) + 1
                pair = pair[1] + letter
        self.measure()

    def measure(self):
        """calculates the scalar length of the trigram vector and stores it in self.length."""
        total = 0
        for y in self.lut.values():
            total += sum([x * x for x in y.values()])
        self.length = total ** 0.5

    def similarity(self, other):
        """returns a number between 0 and 1 indicating similarity.
        1 means an identical ratio of trigrams; 0 means no trigrams in common."""
        if not isinstance(other, Trigram):
            raise TypeError("can't compare Trigram with non-Trigram")
        lut1 = self.lut
        lut2 = other.lut
        total = 0
        for k in lut1.keys():
            if k in lut2:
                a = lut1[k]
                b = lut2[k]
                for x in a:
                    if x in b:
                        total += a[x] * b[x]
        return total / (self.length * other.length)

    def __sub__(self, other):
        """indicates difference between trigram sets; 1 is entirely
        different, 0 is entirely the same."""
        return 1 - self.similarity(other)
