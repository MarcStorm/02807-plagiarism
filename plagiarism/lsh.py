from .util import listhash
from nltk import ngrams


class LSH:

    def __init__(self, datastore, b=20, r=5, q=7):
        self.sigs = {}
        self.b = b  # number of bands
        self.r = r  # number of rows
        self.q = q
        self.k = b * r  # number of minhashes
        self.threshold = (1.0 / b) ** (1.0 / r)
        self.datastore = datastore

    def shingle(self, s):
        '''

        :param s:
        :return:
        '''
        shingles = ngrams(s.split(), self.q)
        return list(shingles)

    def minhash(self, shingles):
        '''

        :param shingles:
        :return:
        '''
        return [min([listhash(s, seed) for s in shingles]) for seed in range(self.k)]

    def signature(self, doc_content):
        '''

        :param doc_content:
        :return:
        '''
        return self.minhash(self.shingle(doc_content))

    def split_list(self, l):
        '''

        :param l:
        :return:
        '''
        for i in range(0, len(l), self.r):
            yield l[i:i + self.r]

    def partition_signature(self, signature):
        '''

        :param signature:
        :return:
        '''
        return [tuple(rows) for rows in self.split_list(signature)]

    def add_document(self, article_id, article):
        '''

        :param article:
        :return:
        '''
        sig = self.signature(article)
        bands = self.partition_signature(sig)
        self.datastore.add_to_matrix(article_id, bands)
