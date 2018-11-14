

def jaccard_estimated(set1, set2, divisor):
    return (len(set1 & set2))/divisor


def jaccard(set1, set2):
    return (len(set1 & set2)) / len(set1 | set2)
