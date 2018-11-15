import sys
import os
import bz2
import xml.etree.ElementTree as xml


class Wiki():
    """
    Wiki is an iterable object which reads articles from a compressed 
    muiltistream dump of wikipedia and decompresses articles
    """

    def __init__(self, article_path=None, index_path=None):
        """
        Creates a new wikipedia instance to read articles

        Args:
            article_path: path to articles *.bz2 file
            index_path: path to index *.txt file

        Note:
            If neither of the files are given (None) the file
            paths will be read from the config.py file
        """
        self.article_path = article_path
        self.index_path = index_path

        if article_path is None or index_path is None:
            self._init_from_config()

        self.index = iter(Index(self.index_path))
        self.articles = Archive(self.article_path)


    def _init_from_config(self):
        try:
            import config
        except ImportError:
            print("Missing configuration file")
            sys.exit(1)
        if self.article_path is None:
            self.article_path = config.WIKI_ARTICLE_PATH
        if self.index_path is None:
            self.index_path = config.WIKI_INDEX_PATH

    def __iter__(self):
        return self


    def __next__(self):
        seek_start, seek_end, docs = next(self.index)
        articles = self.articles._decompress_block(seek_start, seek_end)
        assert len(articles) == len(docs)
        if not os.path.exists("out"):
            os.mkdir("out")
        for i, a in enumerate(articles):
            with open("out/test_{}.xml".format(i), "wb") as f:
                f.write(a)
        print("Saved {} articles to folder ./out".format(len(articles)))
        return docs



class Archive():
    """
    Archive is an object which can read uncompress data from a
    wikipedia multistream data dump.
    """

    DELIMITER = b'</page>'

    def __init__(self, article_path):
        """
        Creates a new wikipedia article decompressor

        Args:
            article_path (str): the path to the wikipedia multistream file
        """
        self.article_path = article_path
        self.archive = open(self.article_path, 'rb')

    def _decompress_block(self, start_block, end_block):
        """
        Decompress a block of articles and return each documents XML

        Return:
            doclist (list): list of bytes for each XML document
        """
        articles = list()
        self.archive.seek(start_block)
        compressed = self.archive.read(end_block-start_block)
        content = bz2.decompress(compressed)

        last_idx = 0
        idx = content.find(Archive.DELIMITER)
        while idx >= 0 and last_idx != idx:
            idx += len(Archive.DELIMITER)
            articles.append(content[last_idx:idx])
            last_idx = idx
            idx = content.find(Archive.DELIMITER, last_idx)
        return articles





class Index():
    """
    Index is an iterable object which read indexes from a wikipedia
    index file in blocks one by one. Also facilitates binary search in index file 
    given an article id amongst other utilities related to the index file.
    """

    def __init__(self, index_path):
        """
        Creates a new wikipedia indexer

        Args:
            index_path (srt): the path to the index file
        """
        self.index_path = index_path
        self.index = iter(open(self.index_path, 'r', encoding='latin-1'))
        self._last_index = None


    @property
    def _last_seek(self):
        """
        Returns the seek value from the last index
        """
        if self._last_index is None:
            return None
        else:
            return self._last_index[0]


    def __iter__(self):
        return self

    
    def __next__(self):
        """
        Returns the next block of indexes

        Returns:
            tuple (start, stop, docs)
            start (int): the block byte start
            stop (int): the block byte end
            docs (list): a list of wiki documents
        """
        return self._get_next_block()


    def _get_doc_info(self, line):
        seek, docid, docname = line.split(":", 2)
        return int(seek), int(docid), str(docname).strip()
    

    def _find_block(self, start_seek, seek_amount=128):
        """
        Finds all documents in a block. The article index pointed to by start_seek
        is included in the returned block. Backpaddles from start_seek in increments
        of seek_amount untill the start of the block is found. On every unsuccessful
        backpaddle the seek_amount is doubled. All articles in the block is then collected.

        Args:
            start_seek (int): seek byte in index file

        Note:
            The start_seek must point to the exact start byte of a line where the article index
            can be found. Not honering so may produce corrupt results.
        """
        with open(self.index_path) as f:
            f.seek(0, 2)
            end = f.tell()
            f.seek(start_seek)
            next_seek = start_seek
            orig_seek, _, _ = self._get_doc_info(f.readline())
            seek = orig_seek

            # Backpaddle to previous block
            while seek == orig_seek:
                next_seek -= seek_amount
                seek_amount *= 2
                f.seek(max(0, next_seek))
                if next_seek <= 0:
                    break
                f.readline()
                seek, _, _ = self._get_doc_info(f.readline())

            # Forward to first document in block
            d = (seek, _, _) = self._get_doc_info(f.readline())
            while seek != orig_seek:
                d = (seek, _, _) = self._get_doc_info(f.readline())
            docs = list()

            # Collect all articles in block
            while seek == orig_seek:
                docs.append(d)
                try:
                    d = (seek, _, _) = self._get_doc_info(f.readline())
                except ValueError:
                    # Except only allowed because of EOF
                    assert f.tell() == end; break
            return (orig_seek, seek, docs)


    def find_article(self, article_id):
        """
        Searches the index file with an article id using binary search in O(log(n)) time.
        Returns the block in which the article is included.

        Args:
            article_id (int): the article id being searched for

        Returns:
            tuple of (start, end, docs)
            start (int): the byte at which the block starts
            end (int): the byte at which the block ends
            docs (list): a list of documents (including the one searched for)
        """
        with open(self.index_path) as f:
            f.seek(0, 2)
            begin = 0
            end = f.tell()

            def traverse():
                f.seek(begin)
                while f.tell() < end:
                    before = f.tell()
                    _, docid, _ = self._get_doc_info(f.readline())
                    if article_id == docid:
                        return self._find_block(before)
                return None

            last = None
            while begin < end:
                if last == (begin, end):
                    return traverse()
                last = (begin, end)
                f.seek(begin+(end-begin)//2, 0)
                f.readline()
                before = f.tell()
                _, docid, _ = self._get_doc_info(f.readline())
                if docid == article_id:
                    return self._find_block(before)
                elif article_id > docid:
                    begin = f.tell()
                else:
                    end = f.tell()



    def _get_next_index(self):
        """
        Reads the next line in the index file

        Returns:
            tuple of (seek, docid, docname) where:
            seek (int): the starting byte for the block of the article
            docid (int): the document id
            docname (str): the name of the document
        """
        return self._get_doc_info(next(self.index))


    def _get_next_block(self):
        docs = list()

        if self._last_index is not None:
            docs.append(self._last_index)

        index = (seek, docid, docname) = self._get_next_index()

        if self._last_index is None:
            self._last_index = index

        while self._last_seek == seek:
            docs.append(index)
            index = (seek, docid, docname) = self._get_next_index()

        seek_range = (self._last_seek, seek, docs)
        self._last_index = index
        return seek_range

    



if __name__ == '__main__':
    import argparse

    def a():
        print("this is a")

    def b():
        print("this is b")

    parser = argparse.ArgumentParser(description='Manipulate wikipedia database.')
    
    subparsers = parser.add_subparsers()

    parser_a = subparsers.add_parser('a', help='a help')
    parser_a.add_argument('bar', type=int, help='bar help')
    parser_a.set_defaults(func=a)

    # create the parser for the "b" command
    parser_b = subparsers.add_parser('b', help='b help')
    parser_b.add_argument('--baz', choices='XYZ', help='baz help')
    parser_b.set_defaults(func=b)

    args = parser.parse_args()

    if args.func is not None:
        args.func(args)

