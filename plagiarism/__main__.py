from .lsh import LSH
from .wiki import Wiki
from .datastore import PickleDatastore
import os

PATH = os.path.dirname(os.path.abspath(__file__))
datastore = PickleDatastore(os.path.join(PATH,'resources/lsh/matrix.pickle'))
lsh = LSH(datastore)
wiki = Wiki()

for i, article in enumerate(wiki.items(filter_redirects=True)):
    print('Adding article with ID: {}'.format(article.id))
    if i > 10:
        break
    lsh.add_document(article.id, article.clean())


'''
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
'''