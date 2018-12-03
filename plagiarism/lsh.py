from util import listhash, split_document
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk
import mmh3
import threading
import re


class LSH:

    def __init__(self, datastore, b=20, r=5, q=9, verbose=False, paragraphs=False):
        self.sigs = {}
        self.b = b  # number of bands
        self.r = r  # number of rows
        self.q = q
        self.k = b * r  # number of minhashes
        self.threshold = (1.0 / b) ** (1.0 / r)
        self.datastore = datastore
        self.verbose = verbose
        self.article_counter = 0
        self.paragraph_counter = 0
        self.paragraphs = paragraphs
        self.lock = threading.Lock()

        # Download necessary resources
        nltk.download('punkt')
        nltk.download('stopwords')


    def clean_document(self, doc):
        doc = re.sub(r'[^a-zA-Z0-9\s]+', '', doc)
        doc = re.sub(r'\s+', ' ', doc).strip()
        doc = doc.lower()
        
        # Remove stop words
        stop_words = set(stopwords.words('english'))
        doc_tokens = word_tokenize(doc)
        doc = [w for w in doc_tokens if w not in stop_words]
        doc = ' '.join(doc)

        return doc


    def shingle(self, s):
        '''
        Shingle will split a text into groups of size q in a list.

        :param s: string to split in singles
        :return: list of shingles
        '''
        shingles = list(ngrams(s.split(), self.q))
        if len(shingles) == 0:
            raise DocumentTooShortError()
        return shingles


    def minhash(self, shingles):
        '''
        Minhas computes a list of the minimum hash values for each shingle.

        :param shingles: list of shingles
        :return: list of minimum hash values, one for each shingle
        '''
        minhashes = list()
        hashes = [listhash(s, 0) for s in shingles]
        for i in range(self.k):
            minhashes.append(min(hashes))
            hashes = [mmh3.hash(e.to_bytes(4, 'little', signed=True), i+1) for e in hashes]
        return minhashes


    def signature(self, doc):
        '''

        :param doc: document to create a signature for.
        :return: list of minimum has values, being the signature.
        '''
        return self.minhash(self.shingle(doc))


    def split_list(self, l):
        '''
        split list will split a list into chunks of r rows.

        :param l: list to split.
        :return: generator.
        '''
        for i in range(0, len(l), self.r):
            yield l[i:i + self.r]


    def partition_signature(self, signature):
        '''

        :param signature: signature to partition.
        :return: a list of tuples, where each tuple is a partition of hash values.
        '''
        return [tuple(rows) for rows in self.split_list(signature)]


    def add_document(self, doc_id, doc):
        '''
        Adds a single document to the LSH
        '''
        doc = self.clean_document(doc)

        if self.paragraphs:
            self._add_paragraphs(doc_id, doc)
        else:
            self._add_document(doc_id, doc)


    def _add_document(self, doc_id, doc):
        '''

        :param doc_id: identifier of the document.
        :param doc: document to add to the signature matrix.
        :return: None
        '''
        self.lock.acquire()

        sig = self.signature(doc)
        bands = self.partition_signature(sig)
        self.datastore.add_to_matrix(doc_id, bands)

        self.article_counter += 1
        if self.verbose:
            print('Added article with ID: {} \t Total articles: {}'.format(doc_id, self.article_counter))

        self.lock.release()

    
    def _add_paragraphs(self, doc_id, doc):
        '''
        Adds a document to the LSH by splitting it into paragraphs
        '''
        self.lock.acquire()

        paragraphs = split_document(doc)

        if len(paragraphs[-1]) < self.q:
            paragraphs = paragraphs[0:-1]

        for p in paragraphs:
            try:
                sig = self.signature(p)
                bands = self.partition_signature(sig)
                self.datastore.add_to_matrix(doc_id, bands)
            except DocumentTooShortError:
                pass
        
        self.article_counter += 1
        self.paragraph_counter += len(paragraphs)

        if self.verbose:
            print('Added article with ID: {} \t Total articles: {}\t New paragraphs: {}\t Total paragraphs: {}'.format(doc_id, self.article_counter, len(paragraphs), self.paragraph_counter))
        
        self.lock.release()


    def find_candidates(self, doc):
        doc = self.clean_document(doc)

        if self.paragraphs:
            return self._find_candidates_paragraphs(doc)
        else:
            return self._find_candidates(doc)


    def _find_candidates(self, doc):
        '''

        :param doc: is the document to find candidates for.
        :return: a list of candidates.
        '''
        sig = self.signature(doc)
        bands = self.partition_signature(sig)
        return self.datastore.find_candidates(bands)


    def _find_candidates_paragraphs(self, doc):
        paragraphs = split_document(doc)

        if len(paragraphs[-1]) < self.q:
            paragraphs = paragraphs[0:-1]
        
        candidates = set()

        for p in paragraphs:
            try:
                sig = self.signature(p)
                bands = self.partition_signature(sig)
                candidates |= set(self.datastore.find_candidates(bands))
            except DocumentTooShortError:
                pass
        
        return list(candidates)



    def set_datastore(self, datastore):
        '''

        :param datastore:
        :return:
        '''
        self.datastore = datastore



class DocumentTooShortError(Exception):
    '''
    Raised when a document is too short to be added to the LSH
    '''
    pass