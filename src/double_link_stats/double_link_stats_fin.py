from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import mwparserfromhell as mwp
import itertools
from mwparserfromhell.string_mixin import StringMixIn
import re
import heapq
import math
import random
import lxml.etree as ET
from bs4 import BeautifulSoup

class MRMostDoubleLinks(MRJob):
    
    OUTPUT_PROTOCOL=JSONValueProtocol
    
    def mapper_join_init(self):
        self.inText = False
        self.begin = False
        self.p = True
        self.count = 0
        self.reservoir = [0] * 100
        self.full = ''
        
    def mapper_join(self, _, line):
        
        line = line.strip()
        if line.find( "<page>" ) != -1:
            self.inText = True
            self.begin = True
        if line.find( "</page>" ) != -1 and self.begin:
            self.inText = False
            self.page = (self.full + " </page> ").strip()
            self.full = ''
            soup = BeautifulSoup(self.page, "lxml")
            texts = soup.findAll("text")
            title = '[[' + soup.find("title").text.lower() + ']]'
            #tree = ET.fromstring(self.page)
            #texts = tree.findall('.//text')
            if title.find(':') < 0:
                for t in texts:
                    if t.text:
                        wikicode = mwp.parse(t.text)
                        links = wikicode.filter_wikilinks()
                        links = [l.lower() for l in links if l.find(':') < 0]
                        ulinks = list(set(links))
                        for i in range(len(ulinks)):
                            l1 = ulinks[i]
                            yield l1, (title, 1 / (10.0 + 1.0*len(ulinks)), ulinks)
                        yield title, (None, 0, ulinks)
        if self.inText:
            self.full = self.full + ' ' + line
            
    def reducer_reverse_index(self, key, values):

        linked_list = []
        link_list = []
        for value in values:
            l1, count1, l1_links = value
            if count1 > 0:
                linked_list.append(value)
            else:
                link_list = value

        if link_list:
            dummy, cdummy, C_all = link_list
            for A_info in linked_list:
                A, counta, A_links = A_info
                for C in C_all:
                    if C not in A_links and C != A:
                        yield sorted([A, C]), counta

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    def init_find_max(self):
        self.heap = []
   
    def map_find_max(self, _, word_count_pairs):
        heapq.heappush(self.heap, word_count_pairs)
        
    def final_find_max(self):
        yield None, heapq.nlargest(100, self.heap)
        
    def reducer_init_find_max(self):
        self.heap = []
   
    def reducer_find_max(self, _, heapin):
        for h in heapin:
            for l in h:
                heapq.heappush(self.heap, l)
        
    def reducer_final_find_max(self):
        yield None, heapq.nlargest(100, self.heap)

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_join_init,
                   mapper=self.mapper_join,
                   reducer=self.reducer_reverse_index),
            MRStep(reducer=self.reducer_count_words),
            MRStep(mapper_init=self.init_find_max,
                   mapper=self.map_find_max,
                   mapper_final=self.final_find_max,
                   reducer_init=self.reducer_init_find_max,
                   reducer=self.reducer_find_max,
                   reducer_final=self.reducer_final_find_max)
        ]

if __name__ == '__main__':
    MRMostDoubleLinks.run()
