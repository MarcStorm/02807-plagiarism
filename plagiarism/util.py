import mmh3
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import os, sys


def listhash(l, seed):
    val = 0
    for e in l:
        val = val ^ mmh3.hash(e, seed)
    return val


def split_document(document, num_words=80, overlap=True):
    words = document.split()
    step = (num_words//2) if overlap else num_words
    return [' '.join(words[i:i+num_words]) for i in range(0, len(words), step)]


def clean_document(doc):
    doc = re.sub(r'[^a-zA-Z0-9\s]+', '', doc)
    doc = re.sub(r'\s+', ' ', doc).strip()
    doc = doc.lower()
    
    # Remove stop words
    stop_words = set(stopwords.words('english'))
    doc_tokens = word_tokenize(doc)
    doc = [w for w in doc_tokens if w not in stop_words]
    doc = ' '.join(doc)
    return doc