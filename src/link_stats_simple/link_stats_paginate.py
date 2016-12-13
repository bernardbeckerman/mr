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

    def mapper_init(self):
        self.page = []

    def mapper(self, _, line):
        self.page.append(line)
        if line.find('</page>') != -1:
            soup = BeautifulSoup('\n'.join(self.page).strip(),'lxml')
            page1 = soup.find('text')
            if page1:
                lset = Set([x.lower for x in mwp.parse(page1.text).filter_wikilinks() if ":" not in x])
                yield None, len(lset)
            self.page = []

if __name__ == '__main__':
    MRMostUsedWord.run()
