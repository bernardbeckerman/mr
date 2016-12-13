import sys
reload(sys)
sys.setdefaultencoding('utf8')
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
            title = soup.find('title').text.lower()
            if ":" not in title:
                lset = list(Set([x.title.lower() for x in mwp.parse(page.text).filter_wikilinks() if ":" not in x]))
                lenl = len(lset)*1.0
                for link in lset:
                    yield link, (title, 1.0/(10.0 + lenl), None)
                yield title, (None, 1.0/(10.0 + lenl), lset)

    def reducer(self, page, links):
        links_from_page = []
        links_to_page = []
        for link in links:
            if link[0] == None:
                links_from_page = link
            else:
                links_to_page.append(link)
        if links_from_page:
            nonelink, weight_from, links_from = links_from_page
            for link_to_page in links_to_page:
                link_to, weight_to, nonelink = link_to_page
                for link_from in links_from:
                    if link_to != link_from:
                        yield sorted([link_to, link_from]), weight_to*weight_from

    def reducer_sumsort_init(self):
        self.heap = []

    def reducer_sumsort(self, AC, weights):
        heapq.heappush(self.heap,(sum(weights),AC))

    def reducer_sumsort_final(self):
        for item in heapq.nlargest(100,self.heap):
            yield None,(item[1],item[0])

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer_init=self.reducer_sumsort_init,
                   reducer=self.reducer_sumsort,
                   reducer_final=self.reducer_sumsort_final)
        ]
if __name__ == '__main__':
    MRMostUsedWord.run()
'''
    def reducer_links_init(self):
        self.ntot  = 0
        self.nsum  = 0
        self.nsum2 = 0
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
