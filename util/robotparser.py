"""
A robot exclusion rules parser for Python by Philip Semanchuk

Full documentation, examples and a comparison to Python's robotparser module reside here:
http://NikitaTheSpider.com/python/rerp/

Comments, bug reports, etc. are most welcome via email to:
   philip@semanchuk.com

Simple usage examples:

    rerp = RobotExclusionRulesParser()
    s = open("robots.txt").read()
    rerp.parse(s)

    if rerp.is_allowed('CrunchyFrogBot', '/foo.html'):
        print "It is OK to fetch /foo.html"

The comments refer to MK1994, MK1996 and GYM2008. These are:
MK1994 = the 1994 robots.txt draft spec (http://www.robotstxt.org/orig.html)
MK1996 = the 1996 robots.txt draft spec (http://www.robotstxt.org/norobots-rfc.txt)
GYM2008 = the Google-Yahoo-Microsoft extensions announced in 2008
(http://www.google.com/support/webmasters/bin/answer.py?hl=en&answer=40360)

This code is released under the following BSD license --

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of robotexclusionrulesparser nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY ITS CONTRIBUTORS ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Philip Semanchuk BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

"""
Modified by Vit Suchomel, 2016-10-20.
Python 3, bytestrings instead of unicode, re2 instead of re (see below),
no robots.txt fetching, no expiration, no fancy functions,...
"""

from urllib.parse import urlparse as urllib_urlparse
from urllib.parse import urlunparse as urllib_urlunparse
from urllib.parse import unquote as urllib_unquote
import re

#Added by VS
#The vanilla re module cannot cope with malicious rules in robots.txt
#such as "Allow: /*********.js$" which would block the re parser forever.
try:
    import re2
    re_escape = re2.escape
except ImportError:
    try:
        import cffi_re2 as re2
    except ImportError:
        raise ImportError('Cannot import re2!')
    from re import escape as re_escape

# These are the different robots.txt syntaxes that this module understands.
# Hopefully this list will never have more than two elements.
MK1996 = 1
GYM2008 = 2

_end_of_line_regex = re.compile('(?:\\r\\n)|\\r|\\n')

# This regex is a little more generous than the spec because it accepts
# "User-agent" or "Useragent" (without a dash). MK1994/96 permits only the
# former. The regex also doesn't insist that "useragent" is at the exact
# beginning of the line, which makes this code immune to confusion caused
# by byte order markers.
_directive_regex = re.compile('(allow|disallow|user[-]?agent|sitemap|crawl-delay):[ \t]*(.*)', re.I)

# Control characters are everything < 0x20 and 0x7f.
_control_characters_regex = re.compile('[\\000-\\037]|\\0177')

def _unquote_path(path):
    # MK1996 says, 'If a %xx encoded octet is encountered it is unencoded
    # prior to comparison, unless it is the "/" character, which has
    # special meaning in a path.'
    if '%' in path:
        return urllib_unquote(path.replace('%2f', '\n').replace('%2F', '\n')).replace('\n', '%2F')
    return path

def _scrub_data(s):
    # Data is either a path or user agent name; i.e. the data portion of a
    # robots.txt line. Scrubbing it consists of (a) removing extraneous
    # whitespace, (b) turning tabs into spaces (path and UA names should not
    # contain tabs), and (c) stripping control characters which, like tabs,
    # shouldn't be present. (See MK1996 section 3.3 "Formal Syntax".)
    return _control_characters_regex.sub('', s).replace('\t', ' ').strip()

class _Ruleset(object):
    """ _Ruleset represents a set of allow/disallow rules (and possibly a
    crawl delay) that apply to a set of user agents.
    """

    __slots__ = ('ALLOW', 'DISALLOW', 'robot_names', 'rules', 'crawl_delay')

    def __init__(self):
        self.robot_names = []
        self.rules = []
        self.crawl_delay = None
        self.ALLOW = 1
        self.DISALLOW = 2

    def add_robot_name(self, bot):
        self.robot_names.append(bot)

    def add_allow_rule(self, path):
        self.rules.append((self.ALLOW, _unquote_path(path)))

    def add_disallow_rule(self, path):
        self.rules.append((self.DISALLOW, _unquote_path(path)))

    def is_not_empty(self):
        return bool(len(self.rules)) and bool(len(self.robot_names))

    def is_default(self):
        return bool('*' in self.robot_names)

    def does_user_agent_match(self, user_agent):
        match = False

        for robot_name in self.robot_names:
            # MK1994 says, "A case insensitive substring match of the name
            # without version information is recommended." MK1996 3.2.1
            # states it even more strongly: "The robot must obey the first
            # record in /robots.txt that contains a User-Agent line whose
            # value contains the name token of the robot as a substring.
            # The name comparisons are case-insensitive."
            match = match or (robot_name == '*') or  \
                             (robot_name.lower() in user_agent.lower())

        return match

    def is_url_allowed(self, url, syntax=GYM2008):
        # Schemes and host names are not part of the robots.txt protocol,
        # so  I ignore them. It is the caller's responsibility to make
        # sure they match.
        _, _, path, parameters, query, fragment = urllib_urlparse(url)
        url_path = _unquote_path(urllib_urlunparse(('', '', path, parameters, query, fragment)))

        allowed = True
        for rule_type, rule_path in self.rules:
            if (syntax == GYM2008) and ('*' in rule_path or rule_path.endswith('$')):
                # GYM2008-specific syntax applies here
                # http://www.google.com/support/webmasters/bin/answer.py?hl=en&answer=40360
                if rule_path.endswith('$'):
                    appendix = '$'
                    rule_path = rule_path[:-1]
                else:
                    appendix = ''
                parts = rule_path.split('*')
                pattern = '%s%s' % \
                    ('.*'.join([re_escape(p) for p in parts]), appendix)
                if re2.match(pattern, url_path):
                    # Ding!
                    allowed = (rule_type == self.ALLOW)
                    break
            else:
                # Wildcards are either not present or are taken literally.
                if url_path.startswith(rule_path):
                    # Ding!
                    allowed = (rule_type == self.ALLOW)
                    # A blank path means "nothing", so that effectively negates the value above.
                    # e.g. "Disallow:   " means allow everything
                    if not rule_path:
                        allowed = not allowed
                    break
        return allowed

class RobotExclusionRulesParser(object):
    """A parser for robots.txt files."""

    __slots__ = ('user_agent', '_response_code', '__rulesets')

    def __init__(self):
        self.user_agent = None
        self._response_code = 0
        self.__rulesets = []

    def is_allowed(self, user_agent, url, syntax=GYM2008):
        """True if the user agent is permitted to visit the URL. The syntax
        parameter can be GYM2008 (the default) or MK1996 for strict adherence
        to the traditional standard.
        """
        for ruleset in self.__rulesets:
            if ruleset.does_user_agent_match(user_agent):
                return ruleset.is_url_allowed(url, syntax)
        return True

    def get_crawl_delay(self, user_agent):
        """Returns a float representing the crawl delay specified for this
        user agent, or None if the crawl delay was unspecified or not a float.
        """
        for ruleset in self.__rulesets:
            if ruleset.does_user_agent_match(user_agent):
                return ruleset.crawl_delay
        return None

    def parse(self, s):
        """Parses the passed string as a set of robots.txt rules."""
        self.__rulesets = []

        # Normalize newlines.
        s = _end_of_line_regex.sub('\n', s)
        lines = s.split('\n')

        previous_line_was_a_user_agent = False
        current_ruleset = None

        for line in lines:
            line = line.strip()

            if line and line[0] == '#':
                # "Lines containing only a comment are discarded completely,
                # and therefore do not indicate a record boundary." (MK1994)
                pass
            else:
                # Remove comments
                i = line.find('#')
                if i != -1:
                    line = line[:i]

                line = line.strip()

                if not line:
                    # An empty line indicates the end of a ruleset.
                    if current_ruleset and current_ruleset.is_not_empty():
                        self.__rulesets.append(current_ruleset)

                    current_ruleset = None
                    previous_line_was_a_user_agent = False
                else:
                    # Each non-empty line falls into one of six categories:
                    # 1) User-agent: blah blah blah
                    # 2) Disallow: blah blah blah
                    # 3) Allow: blah blah blah
                    # 4) Crawl-delay: blah blah blah
                    # 5) Sitemap: blah blah blah
                    # 6) Everything else
                    # 1 - 5 are interesting and I find them with the regex
                    # below. Category 6 I discard as directed by the MK1994
                    # ("Unrecognised headers are ignored.")
                    # Note that 4 & 5 are specific to GYM2008 syntax, but
                    # respecting them here is not a problem. They're just
                    # additional information the the caller is free to ignore.
                    matches = _directive_regex.findall(line)

                    # Categories 1 - 5 produce two matches, #6 produces none.
                    if matches:
                        field, data = matches[0]
                        field = field.lower()
                        data = _scrub_data(data)

                        # Matching "useragent" is a deviation from the
                        # MK1994/96 which permits only "user-agent".
                        if field in ('useragent', 'user-agent'):
                            if previous_line_was_a_user_agent:
                                # Add this UA to the current ruleset
                                if current_ruleset and data:
                                    current_ruleset.add_robot_name(data)
                            else:
                                # Save the current ruleset and start a new one.
                                if current_ruleset and current_ruleset.is_not_empty():
                                    self.__rulesets.append(current_ruleset)
                                #else:
                                    # (is_not_empty() == False) ==> malformed
                                    # robots.txt listed a UA line but provided
                                    # no name or didn't provide any rules
                                    # for a named UA.
                                current_ruleset = _Ruleset()
                                if data:
                                    current_ruleset.add_robot_name(data)

                            previous_line_was_a_user_agent = True
                        elif field == 'allow':
                            previous_line_was_a_user_agent = False
                            if current_ruleset:
                                current_ruleset.add_allow_rule(data)
                        elif field == 'sitemap':
                            previous_line_was_a_user_agent = False
                        elif field == 'crawl-delay':
                            # Only Yahoo documents the syntax for Crawl-delay.
                            # ref: http://help.yahoo.com/l/us/yahoo/search/webcrawler/slurp-03.html
                            previous_line_was_a_user_agent = False
                            if current_ruleset:
                                try:
                                    current_ruleset.crawl_delay = float(data)
                                except ValueError:
                                    # Invalid crawl-delay – ignore.
                                    pass
                        else:
                            # This is a disallow line
                            previous_line_was_a_user_agent = False
                            if current_ruleset:
                                current_ruleset.add_disallow_rule(data)

        if current_ruleset and current_ruleset.is_not_empty():
            self.__rulesets.append(current_ruleset)

        # Now that I have all the rulesets, I want to order them in a way
        # that makes comparisons easier later. Specifically, any ruleset that
        # contains the default user agent '*' should go at the end of the list
        # so that I only apply the default as a last resort. According to
        # MK1994/96, there should only be one ruleset that specifies * as the
        # user-agent, but you know how these things go.
        not_defaults = [r for r in self.__rulesets if not r.is_default()]
        defaults = [r for r in self.__rulesets if r.is_default()]

        self.__rulesets = not_defaults + defaults
