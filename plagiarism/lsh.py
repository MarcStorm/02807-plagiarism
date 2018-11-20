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

    def signature(self, doc):
        '''

        :param doc:
        :return:
        '''
        return self.minhash(self.shingle(doc))

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

    def add_document(self, doc_id, doc):
        '''

        :param doc_id: identifier of the document.
        :param doc: document to add to the signature matrix.
        :return: None
        '''
        sig = self.signature(doc)
        bands = self.partition_signature(sig)
        self.datastore.add_to_matrix(doc_id, bands)

    def find_candidates(self, doc):
        '''

        :param doc: is the document to find candidates for.
        :return: a list of candidates.
        '''
        sig = self.signature(doc)
        bands = self.partition_signature(sig)
        return self.datastore.find_candidates(bands)


    def set_datastore(self, datastore):
        '''

        :param datastore:
        :return:
        '''
        self.datastore = datastore
