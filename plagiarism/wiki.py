import sys
import os
import bz2
import util
from enum import Enum
from contextlib import contextmanager
from functools import lru_cache as cache
import xml.etree.ElementTree as xml
from xml.dom import minidom
import mwparserfromhell
import traceback



class Format(Enum):
    """
    An enumeration of different article formats to export
    """
    XML = "xml"
    TEXT = "text"
    CLEAN = "clean"



class Wiki():
    """
    Wiki is an object which reads articles from a compressed muiltistream
    dump of wikipedia and decompresses articles.

    Summary:
        items(): returns an iterator for the articles in the wiki
        find_article(article_id): find a specific article in the wiki
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

        self.index = Index(self.index_path)
        self.archive = Archive(self.article_path)

        self.current_block = None


    @property
    def config(self):
        try:
            from config import WIKI_ARTICLE_PATH, WIKI_INDEX_PATH
            return locals()
        except ImportError:
            print("Missing configuration file")
            print(traceback.print_exc())
            sys.exit(1)


    def _init_from_config(self):
        if self.article_path is None:
            self.article_path = self.config['WIKI_ARTICLE_PATH']
        if self.index_path is None:
            self.index_path = self.config['WIKI_INDEX_PATH']


    def items(self, **kwargs):
        """
        Returns an iterator for the articles in the wiki

        Args:
            **kwargs: see constructor for _Iterator class
        """
        return Wiki._Iterator(self, **kwargs)


    def find_article(self, article_id):
        """
        Finds a single article with an article_id and returns it

        Args:
            article_id (int): the article id to search for

        Note:
            Raises ArticleNotFound exception when the article could
            not be found
        """
        start, end, _ = self.index.find_article(article_id)
        for content in self.archive.get_block(start, end):
            article = Article(content)
            if article.id == article_id:
                return article
        raise ArticleNotFound("article with id {} not found".format(article_id))


    def __iter__(self):
        """
        Default iterator for wiki articles with no filtering
        """
        return self.items()


    class _Iterator():
        """
        Private iterator class which iterates the articles in the wiki
        given some parameters. Facilitates filtering. An instance of the
        iterator is returned when calling items() on the parented wiki
        instance.
        """

        def __init__(self, wiki, filter_redirects=False):
            """
            Creates a new iterator from a wiki instance. Supports keywords
            used to filter the stream of articles from the wiki.

            Args:
                wiki (Wiki): the wiki instance from which the iterator will run
                filter_redirects (bool): filter out redirected articles
            """
            self.index = Index(wiki.index_path)
            self.archive = Archive(wiki.article_path)

            self.filter_redirects = filter_redirects

            self.current_block = None


        def __iter__(self):
            self.index_iter = iter(self.index)
            return self


        def __next__(self):
            try:
                article = Article(next(self.current_block))
                if self.filter_redirects and article.is_redirect:
                    return self.__next__()
                return article
            except (StopIteration, TypeError):
                seek_start, seek_end, _ = next(self.index_iter)
                self.current_block = self.archive.get_block(seek_start, seek_end)
                return self.__next__()





class Article():
    """
    Article represents a wikipedia article extracted from an archive.
    Used to read and manipulate the content of the article as well as
    gathering metadata from the article.
    """

    def __init__(self, content_xml):
        """
        Creates a new article object from a bytearray which is the XML
        of the the document itself

        Args:
            content_xml (bytearray): the content of the article in XML
        """
        self.content = content_xml
        self.id = int(self.root.find("id").text)
        self.title = str(self.root.find("title").text)


    @property
    @cache(maxsize=1)
    def root(self):
        return xml.fromstring(self.content)


    @property
    def is_redirect(self):
        return self.root.find("redirect") is not None


    def __str__(self):
        s = "id: {:<6d} title: {:<28s} redirect: {:<b}"
        return s.format(self.id, self.title, self.is_redirect)


    def string(self):
        """
        Decodes the XML file to utf-8 and returns a string object
        """
        return self.content.decode(encoding='utf-8')


    def text(self):
        """
        Returns the markup text of the article without XML.
        """
        return str(self.root.find("revision").find("text").text)


    def clean(self):
        """
        Cleans the markup and returns a text-only version of
        the article without any markup, links, html etc.
        """
        code = mwparserfromhell.parse(self.text())
        return code.strip_code(normalize=True, collapse=True)


    def pretty(self):
        """
        Prettyfies the XML markup and indents it with tabs.
        """
        dom = minidom.parseString(self.string())
        return dom.toprettyxml(indent="\t")


    def save(self, file_path):
        """
        Saves the article content to an XML file

        Args:
            file_path (str): path to the XML file to save
        """
        with open(file_path, 'wb') as f:
            f.write(self.content)


    def export(self, fmt=Format.XML, folder='./out', prefix='article'):
        """
        Exports the article into another format and writes it to a file
        """
        extensions = {
            Format.XML: "xml",
            Format.TEXT: "txt",
            Format.CLEAN: "txt",
        }
        if not os.path.exists(args.out):
            os.mkdir(args.out)
        name = "{}_{}.{}".format(prefix, self.id, extensions[fmt])
        with open(os.path.join(folder, name), 'wb') as f:
            if fmt == Format.XML:
                f.write(self.content)
            elif fmt == Format.TEXT:
                f.write(self.text().encode('utf-8'))
            elif fmt == Format.CLEAN:
                f.write(self.clean().encode('utf-8'))
            else:
                raise ValueError("{} is not a regocnized format".format(fmt))




class Archive():
    """
    Archive is an object which can read compressed data from a
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


    def get_block(self, start_block, end_block):
        """
        Decompress a block of articles and return each documents XML

        Return:
            doclist (iter): iterable sequence of bytearrays for each XML document
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
        return iter(articles)





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

    @contextmanager
    def open(self):
        f = open(self.index_path, 'r', encoding='latin-1')
        try:
            yield f
        finally:
            f.close()


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
        with self.open() as f:
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

        Raises:
            ArticleNotFound : if the article id was not found in the index
        """
        with self.open() as f:
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
                raise ArticleNotFound("article id {} not found in index".format(article_id))

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



class ArticleNotFound(Exception):
    """
    Raisen when no article could be found
    """
    pass



if __name__ == '__main__':
    import argparse

    wiki = Wiki()

    def cmd_list(args):
        items = wiki.items(filter_redirects=args.direct)
        for i, article in enumerate(items):
            if i >= args.limit:
                break
            if args.mode == 'stdout':
                print("{}\t{}".format(article.id, util.clean_document(article.clean())))
            elif args.out is not None:
                if not args.quiet:
                    print(article)
                article.export(folder=args.out, prefix=args.prefix, fmt=Format(args.mode))


    def cmd_find(args):
        article = wiki.find_article(args.id)
        if not args.quiet:
            print(article)
        if args.out is not None:
            article.export(folder=args.out, prefix=args.prefix, fmt=Format(args.mode))


    # Main parser
    parser = argparse.ArgumentParser(
        description='Manipulate wikipedia database.'
    )

    # Common parser (common flags, used for inheritance)
    parse_common = argparse.ArgumentParser(add_help=False)
    parse_common.add_argument('-q', '--quiet', action='store_true', help='omit output to stdout')

    mode_group = parse_common.add_mutually_exclusive_group()
    mode_group.add_argument('-X', '--xml', help="set write mode to xml", action='store_const', const='xml', dest='mode')
    mode_group.add_argument('-T', '--txt', help="set write mode to text", action='store_const', const='text', dest='mode')
    mode_group.add_argument('-C', '--clean', help="set write mode to clean", action='store_const', const='clean', dest='mode')
    mode_group.add_argument('-O', '--stdout', help="set write mode to stdout", action='store_const', const='stdout', dest='mode')

    parse_common.add_argument('--prefix', metavar='P', type=str, help='prefix P to filenames', default='article')

    subparsers = parser.add_subparsers(help='commands')

    # Parser for ls command
    parse_list = subparsers.add_parser('ls', help='list articles in order', parents=[parse_common])
    parse_list.add_argument('-l', '--limit', type=int, metavar='N', help='limit to first N articles', default=max)
    parse_list.add_argument('-d', '--direct', action='store_true', help='filter out redirected articles')
    parse_list.add_argument('out', type=str, metavar='FOLDER', help='extract articles to folder', nargs='?')
    parse_list.set_defaults(func=cmd_list)

    # Parser for find command
    parse_find = subparsers.add_parser('find', help='find a single article by id', parents=[parse_common])
    parse_find.add_argument('id', type=int, metavar='ID', help='article id to search for')
    parse_find.add_argument('out', type=str, metavar='FOLDER', help='extract article to folder', nargs='?')
    parse_find.set_defaults(func=cmd_find)

    args = parser.parse_args()

    if 'func' in args:
        args.func(args)
    else:
        parser.print_help()
