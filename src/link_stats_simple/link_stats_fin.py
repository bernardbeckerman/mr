from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import mwparserfromhell as mwp
from mwparserfromhell.string_mixin import StringMixIn
import re
import heapq
import math
import random
import lxml.etree as ET
from bs4 import BeautifulSoup

class MRMostUsedWords(MRJob):
    
    OUTPUT_PROTOCOL=JSONValueProtocol
    
    def mapper_init(self):
        self.inText = False
        self.begin = False
        self.p = True
        self.count = 0
        self.reservoir = [0] * 100
        self.full = ''
        
    def mapper(self, _, line):
        
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
            #tree = ET.fromstring(self.page)
            #texts = tree.findall('.//text')
            for t in texts:
                if t.text:
                    wikicode = mwp.parse(t.text)
                    links = wikicode.filter_wikilinks()
                    print links
                    links = [l.lower() for l in links]
                    ulinks = list(set(links))
                    #yield None, len(ulinks)
        if self.inText:
            self.full = self.full + ' ' + line

if __name__ == '__main__':
    MRMostUsedWords.run()
