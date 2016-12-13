#!/usr/bin/python
# Copyright 2009-2010 Yelp
# Copyright 2013 David Marin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

    def mapper_count_words(self, _, line):
        if line.strip() != "":
            soup = BeautifulSoup(line,'lxml')
            pages = soup.find_all('text')
            for page in pages:
                full = mwp.parse(page.text)
                for word in WORD_RE.findall(full.strip_code()):
                    yield (word.lower(), 1)
        
    def combiner_count_words(self, word, counts):
        # sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts),word)

    # discard the key; it is just None
    def reducer_find_nmax_words(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        nw = 100
        heap = []
        for  word_count_pair in word_count_pairs:
            if len(heap) < nw:
                heapq.heappush(heap, word_count_pair) 
            else:
                heapq.heappushpop(heap, word_count_pair)
        for item in heap:
            yield None, item
            
    def reducer_find_nmax_words_final(self, key, word_count_pairs):
        if key == None:
            yield word_count_pairs[0],word_count_pairs[1]
        else:
            yield key, word_count_pairs

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_nmax_words)
        ]


if __name__ == '__main__':
    MRMostUsedWord.run()
