import mmh3
from nltk import ngrams


class LocalitySensitiveHashing:

    def __init__(self, b=20, r=5, q=7):
        self.sigs = {}
        self.b = b  # number of bands
        self.r = r  # number of rows
        self.q = q
        self.k = b * r  # number of minhashes
        self.threshold = (1.0 / b) ** (1.0 / r)

    def listhash(self, l, seed):
        val = 0
        for e in l:
            val = val ^ mmh3.hash(e, seed)
        return val

    def shingle(self, s):
        shingles = ngrams(s.split(), self.q)
        return list(shingles)

    def minhash(self, shingles, k):
        return [min([self.listhash(s, seed) for s in shingles]) for seed in range(k)]

    def signature_document(self, doc_content):
        return self.minhash(self.shingle(self.q, doc_content), self.k)

    def split_list(self, l, chunks):
        for i in range(0, len(l), chunks):
            yield l[i:i + chunks]

    def partition_signature(self, signature):
        return [tuple(rows) for rows in self.split_list(signature, self.r)]

    def signature_matrix(self, signatures):

        matrix = [dict() for i in range(self.b)]

        for (key, value) in signatures.items():
            new_value = key
            for (i, bucket_key) in enumerate(self.partition_signature(value)):
                # Check if key exists, if not create empty list and append value
                # otherwise it will get the exsisting list and append value.
                if bucket_key in matrix[i]:
                    matrix[i][bucket_key].append(new_value)
                else:
                    matrix[i][bucket_key] = [new_value]

        return matrix

    def candidates(self, signature, signature_matrix):

        # List for candidates.
        c = list()

        bands = self.partition_signature(signature)

        for i, key in enumerate(bands):
            c.append(signature_matrix[i].get(key, list()))

        c = [e for l in c for e in l]

        return list(set(c))