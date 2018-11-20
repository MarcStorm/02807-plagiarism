import mmh3

def listhash(l, seed):
    val = 0
    for e in l:
        val = val ^ mmh3.hash(e, seed)
    return val