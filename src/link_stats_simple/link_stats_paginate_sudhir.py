from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol,RawValueProtocol
from mrjob.step import MRStep
from bs4 import BeautifulSoup
import mwparserfromhell as mwp

class MRMostUsedWord(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper_init(self):
        self.page = []
        self.tbool = False

    def mapper(self, _, line):
        line = line.strip()
        self.tbool = self.tbool or ('<page>' in line)
        if self.tbool:
            self.page.append(line)
        if ('</page>' in line) and self.tbool:
            self.tbool = False
            soup = BeautifulSoup(' '.join(self.page).strip(),'lxml')
            pages = soup.find_all('text')
            for page1 in pages:
                if page1.text:
                    lset = list(set([x.lower() for x in mwp.parse(page1.text).filter_wikilinks()]))
                    yield None, len(lset)
            self.page = []

if __name__ == '__main__':
    MRMostUsedWord.run()
