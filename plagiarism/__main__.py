import lsh
import os
import pickle

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