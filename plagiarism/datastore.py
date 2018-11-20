# Imports specific for Pickle
import pickle

# Imports specific for SQL
import sqlite3
from contextlib import contextmanager

# General imports
import os
from .util import listhash
import threading


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


    def __init__(self, db_path, force):
        '''

        :param db_path:
        '''
        self.lock = threading.Lock()
        self.db_path = db_path
        self.db = None

        if os.path.exists(db_path):
            if force:
                os.remove(db_path)
                self.load_datastore()
                self._create_database()
            else:
                self.load_datastore()
        else:
            self.load_datastore()
            self._create_database()


    @contextmanager
    def cursor(self):
        '''

        :return:
        '''
        cursor = self.db.cursor()
        try:
            yield cursor
        finally:
            self.db.commit()


    def add_to_matrix(self, doc_id, bands):
        '''

        :param doc_id:
        :param bands:
        :return:
        '''

        for i,band in enumerate(bands):

            b = [b.to_bytes(4, 'little', signed=True) for b in band]

            h = listhash(b, 0)
            self._insert_hash(h, i, doc_id)


    def find_candidates(self, bands):
        '''

        :param bands:
        :return:
        '''

        c_list = set()

        for i, band in enumerate(bands):

            b = [b.to_bytes(4, 'little', signed=True) for b in band]
            h = listhash(b, 0)

            sql = '''SELECT doc_id FROM documents NATURAL JOIN hashes
                        WHERE hashes.hash = ? AND hashes.band = ?'''

            with self.cursor() as c:
                c.execute(sql, (h, i))
                rows = c.fetchall()
                c_list |= set([row[0] for row in rows])

        return list(c_list)

    def load_datastore(self):
        '''

        :return:
        '''

        self.db = sqlite3.connect(self.db_path)


    def _close(self):
        '''

        :return:
        '''

        self.db.close()


    def _create_database(self):
        '''

        :return:
        '''

        sql_hashes = '''CREATE TABLE hashes(
                            id INTEGER PRIMARY KEY AUTOINCREMENT, 
                            hash BINARY(4) NOT NULL,
                            band INTEGER NOT NULL,
                            CONSTRAINT unq UNIQUE (hash, band)
                        )'''
        sql_documents = '''CREATE TABLE documents(
                            id INTEGER NOT NULL,
                            doc_id INTEGER NOT NULL,
                            PRIMARY KEY (id, doc_id),
                            FOREIGN KEY(id) REFERENCES hashes(id)
                           )'''
        sql_index_hashes = 'CREATE INDEX idx_hashes ON hashes(hash, band)'
        sql_index_documents = 'CREATE INDEX idx_documents ON documents(id)'

        with self.cursor() as c:
            c.execute(sql_hashes)
            c.execute(sql_documents)
            c.execute(sql_index_hashes)
            c.execute(sql_index_documents)

    def _insert_hash(self, hash, band, doc_id):
        '''

        :param hash:
        :param band:
        :param doc_id:
        :return:
        '''

        sql_hashes = '''INSERT OR IGNORE INTO hashes(hash, band) VALUES (?, ?)'''
        sql_documents = '''INSERT INTO documents(id, doc_id) 
                            VALUES ((SELECT id FROM hashes WHERE hash = ? AND band = ?), ?)'''

        with self.cursor() as c:
            c.execute(sql_hashes, (hash, band))
            c.execute(sql_documents, (hash, band, doc_id))
