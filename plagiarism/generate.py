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
            ),
            MRStep(
                mapper=self.mapper_datastore,
                reducer=self.reducer_datastore,
                reducer_final=self.reducer_datastore_final,
            ),
        ]


    def mapper_articles(self, article_id, article):
        article = lsh.clean_document(article)
        paras = util.split_document(article)
        for p in paras:
            yield article_id, p


    def reducer_minhash(self, article_id, para):
        for p in para:
            try:
                sig = lsh.signature(p)
                bands = lsh.partition_signature(sig)
                yield article_id, bands
            except DocumentTooShortError:
                pass


    def mapper_datastore(self, article_id, bands):
        yield None, (article_id, bands)
    
    
    def reducer_datastore(self, _, items):
        for item in items:
            (article_id, bands) = item
            lsh.datastore.add_to_matrix(article_id, bands)


    def reducer_datastore_final(self):
        yield 'done', None



if __name__ == '__main__':
    GeneratorMapReducer.run()
