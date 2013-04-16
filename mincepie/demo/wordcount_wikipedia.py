"""
Wordcount demo from randomly sampled documents from wikipedia

This demo code shows a slightly more complex example: downloading random docs 
from wikipedia, and then count the corresponding words there. Try running with
something like 1000 documents to see what are the most frequent words in
English!

From one run I got the top 5 as this:

('the', 1549)
('of', 1037)
('and', 790)
('in', 729),
('=', 652)

which sort of makes sense...

Running this demo is similar to running wordcount.py, with the only difference
being that you need to specify --input=N where N is the number of documents you
want to download from wikipedia.
"""

from collections import defaultdict
from mincepie import mapreducer
from mincepie import launcher
import re
import urllib2


class WikipediaMapper(mapreducer.BasicMapper):
    """The wordcount mapper"""
    @staticmethod
    def get_random_wikipedia_article():
        """
        Downloads a randomly selected Wikipedia article (via
        http://en.wikipedia.org/wiki/Special:Random) and strips out (most
        of) the formatting, links, etc. 
        
        This code is borrowed from David Blei's online LDA code.
        """
        failed = True
        while failed:
            articletitle = None
            failed = False
            try:
                req = urllib2.Request(\
                        'http://en.wikipedia.org/wiki/Special:Random',
                        None, { 'User-Agent' : 'x'})
                f = urllib2.urlopen(req)
                while not articletitle:
                    line = f.readline()
                    result = re.search(r'title="Edit this page" href="/w/index.php\?title=(.*)\&amp;action=edit" /\>', line)
                    if (result):
                        articletitle = result.group(1)
                        break
                if len(line) > 0:
                    req = urllib2.Request('http://en.wikipedia.org/w/index.php?title=Special:Export/%s&action=submit' \
                            % (articletitle), None, { 'User-Agent' : 'x'})
                    f = urllib2.urlopen(req)
                    all = f.read()
                else:
                    continue
            except (urllib2.HTTPError, urllib2.URLError):
                print 'oops. there was a failure downloading %s. retrying...' \
                    % articletitle
                failed = True
                continue
            # print 'downloaded %s. parsing...' % articletitle
            try:
                all = re.search(r'<text.*?>(.*)</text', all, flags=re.DOTALL).group(1)
                all = re.sub(r'\n', ' ', all)
                all = re.sub(r'\{\{.*?\}\}', r'', all)
                all = re.sub(r'\[\[Category:.*', '', all)
                all = re.sub(r'==\s*[Ss]ource\s*==.*', '', all)
                all = re.sub(r'==\s*[Rr]eferences\s*==.*', '', all)
                all = re.sub(r'==\s*[Ee]xternal [Ll]inks\s*==.*', '', all)
                all = re.sub(r'==\s*[Ee]xternal [Ll]inks and [Rr]eferences==\s*', '', all)
                all = re.sub(r'==\s*[Ss]ee [Aa]lso\s*==.*', '', all)
                all = re.sub(r'http://[^\s]*', '', all)
                all = re.sub(r'\[\[Image:.*?\]\]', '', all)
                all = re.sub(r'Image:.*?\|', '', all)
                all = re.sub(r'\[\[.*?\|*([^\|]*?)\]\]', r'\1', all)
                all = re.sub(r'\&lt;.*?&gt;', '', all)
            except:
                # Something went wrong, try again. (This is bad coding practice.)
                print 'oops. there was a failure. retrying...'
                failed = True
                continue
        return all

    def map(self, key, value):
        """The map function. For every call, we download a random article. We
        do a pre-count inside the document so we do not generate multiple 1
        counts for the same word, which helps the reducer a little bit.
        """
        data = WikipediaMapper.get_random_wikipedia_article()
        counts = defaultdict(int)
        for word in data.split():
            counts[word] += 1
        for key in counts:
            yield key, counts[key]

mapreducer.REGISTER_DEFAULT_MAPPER(WikipediaMapper)


class WordCountReducer(mapreducer.BasicReducer):
    """The wordcount reducer"""
    def reduce(self, key, value):
        return sum(value)

mapreducer.REGISTER_DEFAULT_REDUCER(WordCountReducer)

# we need IterateReader instead of the default reader.
mapreducer.REGISTER_DEFAULT_READER(mapreducer.IterateReader)

if __name__ == "__main__":
    launcher.launch()
