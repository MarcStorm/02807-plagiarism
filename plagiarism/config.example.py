import os

PATH = os.path.dirname(os.path.realpath(__file__))

"""
Path to compressed multistream article dump of wikipedia

Example:
    enwiki-20181020-pages-articles-multistream.xml.bz2
"""
WIKI_ARTICLE_PATH=None

"""
Path to uncompressed index file of wikipedia

Example:
    enwiki-20181020-pages-articles-multistream-index.txt
"""
WIKI_INDEX_PATH=None

"""
Path to SQLite database

Example:
    /home/johndoe/src/matrix.sqlite
"""
SQLITE_PATH=os.path.join(PATH, "resources/lsh/matrix.sample.sqlite")