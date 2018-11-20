import pickle
import os

class Datastore:

    def __init__(self):
        pass

    def add_to_matrix(self, doc_id, bands):
        raise NotImplementedError

    def find_candidates(self, bands):
        raise NotImplementedError

    def load_datastore(self, path):
        raise NotImplementedError



class PickleDatastore(Datastore):


    def __init__(self, pickle_path):
        '''

        :param pickle_path: complete path to pickle file.
        '''
        self.pickle_path = pickle_path
        self.sigs_matrix = None

        if os.path.exists(pickle_path):
            self.load_datastore(pickle_path)


    def add_to_matrix(self, doc_id, bands):
        '''

        :param doc_id:
        :param bands:
        :return:
        '''

        if self.sigs_matrix is None:
            self.sigs_matrix = [dict() for i in range(len(bands))]

        for (i, bucket_key) in enumerate(bands):
            # Check if key exists, if not create empty list and append value
            # otherwise it will get the exsisting list and append value.
            if bucket_key in self.sigs_matrix[i]:
                self.sigs_matrix[i][bucket_key].append(doc_id)
            else:
                self.sigs_matrix[i][bucket_key] = [doc_id]

        self._save_pickle()


    def find_candidates(self, bands):
        '''
        :param bands: is the partitioned signature
        :return: list of candidates
        '''

        c = list()

        for i, key in enumerate(bands):
            c.append(self.sigs_matrix[i].get(key, list()))

        c = [e for l in c for e in l]

        return list(set(c))


    def load_datastore(self, path):
        '''

        :param path:
        :return:
        '''
        self.sigs_matrix = pickle.load(open(path, "rb"))


    def _save_pickle(self):
        '''

        :return:
        '''
        with open(self.pickle_path, 'wb') as f:
            pickle.dump(self.sigs_matrix, f)



class SQLiteDatastore(Datastore):


    def __init__(self):
        pass


    def add_to_matrix(self, doc_id, bands):
        pass


    def find_candidates(self, bands):
        pass


    def load_datastore(self, path):
        pass


'''
OLD CANDIDATES METHOD
'''
'''
def candidates(self, signature, signature_matrix):
    # List for candidates.
    c = list()

    bands = self.partition_signature(signature)

    for i, key in enumerate(bands):
        c.append(signature_matrix[i].get(key, list()))

    c = [e for l in c for e in l]

    return list(set(c))
'''
'''
OLD SIGNATURE_MATRIX
'''
'''
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
'''
'''
# read data sets
srcfolder = os.path.dirname(os.path.abspath(__file__))
data_folder_name = 'ats_corpus_small'
datafolder = os.path.join(srcfolder, data_folder_name)   # change to ats_corpus for large data set
outfile = 'sigs_{}.pickle'.format(data_folder_name)

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
'''