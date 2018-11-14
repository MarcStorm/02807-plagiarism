import os
import mmh3
from nltk import ngrams
import pickle

# ------------------- Utilities -------------------

'''hashes a list of strings'''


def listhash(l, seed):
	val = 0
	for e in l:
		val = val ^ mmh3.hash(e, seed)
	return val


def shingle(q, s):
	shingles = ngrams(s.split(), q)
	return list(shingles)


def minhash(shingles,k):
	return [min([listhash(s, seed) for s in shingles]) for seed in range(k)]


def signatures(docs, q, k):
	new_docs = {}
	for key, value in docs.items():
		new_docs[key] = minhash(shingle(q, value), k)
		print('Processed document: {}'.format(key))
	return new_docs


def signature_document(doc_content, q, k):
	return minhash(shingle(q, doc_content), k)


def jaccard(doc1_signature, doc2_signature):

	doc1_signature = set(doc1_signature)
	doc2_signature = set(doc2_signature)

	return (len(doc1_signature & doc2_signature))/k


def split_list(l, chunks):
	for i in range(0, len(l), chunks):
		yield l[i:i+chunks]


def partition_signature(signature):
	return [tuple(rows) for rows in split_list(signature, r)]


def signature_matrix(signatures, b, r):

	matrix = [dict() for i in range(b)]

	for (key, value) in signatures.items():
		new_value = key
		for (i, bucket_key) in enumerate(partition_signature(value)):
			# Check if key exists, if not create empty list and append value
			# otherwise it will get the exsisting list and append value.
			if bucket_key in matrix[i]:
				matrix[i][bucket_key].append(new_value)
			else:
				matrix[i][bucket_key] = [new_value]

	return matrix


def candidates(signature, signature_matrix, q = 7, k = 100):

	# List for candidates.
	c = list()

	bands = partition_signature(signature)

	for i,key in enumerate(bands):
		c.append(signature_matrix[i].get(key, list()))

	c = [e for l in c for e in l]

	return list(set(c))

# ------------------- Similarity -------------------


docs = {}  # dictionary mapping document id to document contents
sigs = {}
b = 20  # number of bands
r = 5  # number of rows
threshold = (1.0/b)**(1.0/r)
q = 7  # length of shingle
k = b*r  # number of minhashes

# read data sets
srcfolder = os.path.dirname(os.path.abspath(__file__))
data_folder_name = 'ats_corpus_small'
datafolder = os.path.join(srcfolder, data_folder_name)   # change to ats_corpus for large data set
outfile = 'sigs_{}.pickle'.format(data_folder_name)
# for comb in combinations(list(sigs.keys()), 2):
#	print('Similarity of {} and {} is: {}'.format(comb[0], comb[1], jaccard(comb[0], comb[1])))

if os.path.exists(outfile):
	sigs = pickle.load(open(outfile, "rb"))
else:
	for file in os.listdir(datafolder):
		filepath = os.path.join(datafolder, file)
		f = open(filepath, 'r')
		docs[file] = f.read()
		print('read document ' + file)
		f.close()

	print('Create signatures')
	sigs = signatures(docs, q, k)
	with open(outfile, 'wb') as f:
		pickle.dump(sigs,f)

doc_content = open(os.path.join(datafolder, 'calltounconv00baxt.txt'), 'r').read()

print('Creating matrix')
matrix = signature_matrix(sigs, b, r)

print('Creating signature')
sig = signature_document(doc_content, q, k)

print('Creating candidates_list')
candidates_list = candidates(sig, matrix)

print('Starting candidate loop.')
for c in candidates_list:
	print('Candidate {} has a similarity of: {}'.format(c, jaccard(sig, sigs[c])))
