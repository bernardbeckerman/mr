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
from sets import Set

class MRMostUsedWord(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, line):
        if line.strip() != "":
            soup = BeautifulSoup(line.strip(),'lxml')
            page = soup.find('text')
            lset = Set([x.lower() for x in mwp.parse(page.text).filter_wikilinks() if ":" not in x])
            yield None, len(lset)

'''
    def reducer_links_init(self):
        self.ntot  = 0
        self.nsum  = 0
        self.nsum2 = 0

    def reducer_links(self, title, lset):
        lsum = sum(lset)
        self.ntot  += 1
        self.nsum  += lsum
        self.nsum2 += lsum*lsum

    def reducer_links_final(self):
        print self.ntot, self.nsum, self.nsum2

    def steps(self):
        return [
            MRStep(mapper=self.mapper_links,
                   reducer_init=self.reducer_links_init,
                   reducer=self.reducer_links,
                   reducer_final=self.reducer_links_final)
        ]
'''
if __name__ == '__main__':
    MRMostUsedWord.run()
