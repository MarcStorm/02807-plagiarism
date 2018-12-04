from mrjob.job import MRJob
from mrjob.step import MRStep
import util
import os
from datastore import SQLiteDatastore
import config
import random

SPACE = u' '

PATH = os.path.dirname(os.path.abspath(__file__))

from lsh import LSH, DocumentTooShortError
datastore = SQLiteDatastore(config.SQLITE_PATH, False)
lsh = LSH(datastore)

class CandidatesMapReducer(MRJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.remaining = str()

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_paragraphs,
                mapper_final=self.mapper_paragraphs_final,
                reducer=self.reducer_minhash,
            ),
            MRStep(
                reducer=self.reducer_unique,
            ),
        ]

    def mapper_paragraphs(self, _, line):
        line = lsh.clean_document(line)
        para = util.split_document(SPACE.join([line, self.remaining]))
        for p in para[0:-1]:
            yield random.randint(0, 1<<32), p
        if len(para) > 0:
            self.remaining = para[-1]


    def mapper_paragraphs_final(self):
        yield random.randint(0, 1<<32), self.remaining


    def reducer_minhash(self, _, paras):
        candidates = set()
        for p in paras:
            try:
                candidates |= set(lsh._find_candidates(p))
            except DocumentTooShortError:
                pass
        yield None, list(candidates)

    
    def reducer_unique(self, _, cands):
        candidates = set()
        for c in cands:
            candidates |= set(c)
        yield None, list(candidates)


if __name__ == '__main__':
    CandidatesMapReducer.run()
