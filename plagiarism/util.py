import mmh3

def listhash(l, seed):
    val = 0
    for e in l:
        val = val ^ mmh3.hash(e, seed)
    return val


def split_document(document, num_words=125, overlap=True):
    words = document.split()
    step = (num_words//2) if overlap else num_words
    return [' '.join(words[i:i+num_words]) for i in range(0, len(words), step)]
