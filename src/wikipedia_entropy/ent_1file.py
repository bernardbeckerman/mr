import numpy as np
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol,RawValueProtocol
from mrjob.step import MRStep
from operator import itemgetter
import re
import heapq
from lxml import etree
from bs4 import BeautifulSoup
import mwparserfromhell as mwp

WORD_RE = re.compile(r"[\w]+")

class MRMostUsedWord(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol
    INPUT_PROTOCOL = RawValueProtocol

    def mapper_count_ngram_init(self):
        self.cdict = {}

    def mapper_count_ngram(self, _, line):
        if line.strip() != "":
            soup = BeautifulSoup(line,'lxml')
            pages = soup.find_all('text')
            for page in pages:
                full = mwp.parse(page.text)
                text = " ".join(" ".join(fragment.value.split())
                                for fragment in full.filter_text())
                for cchar in text:
                    if cchar not in self.cdict:
                        self.cdict[cchar] = 0
                    self.cdict[cchar] += 1
            for key, value in self.cdict.iteritems():
                yield (key, value)
        
    def reducer_count_ngram(self, ngram, count):
        yield (ngram, sum(count))

    def reducer_entropy_init(self):
        self.ntot  = 0
        self.nlogn = 0

    def reducer_entropy(self, ngram, counts):
        for count in counts:
            self.ntot += count
            self.nlogn += count*np.log2(count)

    def reducer_entropy_final(self):
        yield None, (np.log2(self.ntot) - self.nlogn/self.ntot)

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_count_ngram_init,
                   mapper=self.mapper_count_ngram,
                   reducer=self.reducer_count_ngram),
            MRStep(reducer_init=self.reducer_entropy_init,
                   reducer=self.reducer_entropy,
                   reducer_final=self.reducer_entropy_final)
        ]

if __name__ == '__main__':
    MRMostUsedWord.run()
