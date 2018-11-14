import sys
import os
import bz2
import xml.etree.ElementTree as xml

try:
    import config
except ImportError:
    print("Please configure config file")
    sys.exit(1)


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
        self.article_path = article_path if article_path is not None \
            else config.WIKI_ARTICLE_PATH
        self.index_path = index_path if index_path is not None \
            else config.WIKI_INDEX_PATH

        self.index = iter(WikiIndex(self.index_path))
        self.articles = WikiArchive(self.article_path)


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





class WikiArchive():
    """
    WikiArchive is an object which can read uncompress data from a
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
        idx = content.find(WikiArchive.DELIMITER)
        while idx >= 0 and last_idx != idx:
            idx += len(WikiArchive.DELIMITER)
            articles.append(content[last_idx:idx])
            last_idx = idx
            idx = content.find(WikiArchive.DELIMITER, last_idx)
        return articles





class WikiIndex():
    """
    WikiIndex is an iterable object which read indexes from a wikipedia
    index file in blocks one by one.
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


    def _get_next_index(self):
        """
        Reads the next line in the index file

        Returns:
            tuple of (seek, docid, docname) where:
            seek (int): the starting byte for the block of the article
            docid (int): the document id
            docname (str): the name of the document
        """
        seek, docid, docname = next(self.index).split(":", 2)
        return int(seek), int(docid), str(docname).strip()


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
    wiki = Wiki()
    print("Article: {}".format(wiki.article_path))
    print("Index: {}".format(wiki.index_path))
    next(iter(wiki))
