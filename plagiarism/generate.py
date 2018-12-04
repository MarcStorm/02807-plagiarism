from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import util
import os
from datastore import SQLiteDatastore
import config
from lsh import LSH, DocumentTooShortError

datastore = SQLiteDatastore(config.SQLITE_PATH, False)
lsh = LSH(datastore, paragraphs=True)

class GeneratorMapReducer(MRJob):

    INPUT_PROTOCOL = TextProtocol

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.remaining = str()


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_articles,
                reducer=self.reducer_minhash,
            )
        ]


    def mapper_articles(self, article_id, article):
        paras = util.split_document(article)
        for p in paras:
            yield article_id, p


    def reducer_minhash(self, article_id, paras):
        for p in paras:
            try:
                sig = lsh.signature(p)
                bands = lsh.partition_signature(sig)
                lsh.datastore.add_to_matrix(article_id, bands)
            except DocumentTooShortError:
                pass
        yield 'article', article_id
    


if __name__ == '__main__':
    GeneratorMapReducer.run()
