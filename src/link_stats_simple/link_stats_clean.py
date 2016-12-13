from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol,RawValueProtocol
from mrjob.step import MRStep
from bs4 import BeautifulSoup
import mwparserfromhell as mwp
from sets import Set

class MRMostUsedWord(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, line):
        if line.strip() != "":
            soup = BeautifulSoup(line.strip(),'lxml')
            page = soup.find('text')
            lset = Set([x.lower for x in mwp.parse(page.text).filter_wikilinks() if ":" not in x])
            yield None, len(lset)

if __name__ == '__main__':
    MRMostUsedWord.run()
