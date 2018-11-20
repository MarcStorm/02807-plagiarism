from .lsh import LSH
from .wiki import Wiki
from .metrics import jaccard_estimated, jaccard
from .datastore import PickleDatastore, SQLiteDatastore
import os




'''
article = wiki.find_article(12).clean()
id_list = lsh.find_candidates(article)
print('Starting candidate loop.')
for i in id_list:
    print('Candidate was found with ID: {}'.format(i))
    set1 = set(article.split(' '))
    set2 = set(wiki.find_article(i).clean().split(' '))
    print('Jaccard estimated = {}'.format(jaccard_estimated(set1, set2, len(set1))))
    print('Jaccard = {}'.format(jaccard(set1, set2)))
'''






if __name__ == '__main__':
    import argparse

    PATH = os.path.dirname(os.path.abspath(__file__))
    #datastore = PickleDatastore(os.path.join(PATH, 'resources/lsh/matrix.pickle'))
    datastore = SQLiteDatastore(os.path.join(PATH, 'resources/lsh/matrix.sqlite'))
    lsh = LSH(datastore)
    wiki = Wiki()

    def cmd_gen(args):

        for i, article in enumerate(wiki.items(filter_redirects=True)):
            if i >= args.limit:
                break
            if not args.quiet:
                print('Adding article with ID: {}'.format(article.id))
            lsh.add_document(article.id, article.clean())




    def cmd_lookup(args):

        content = None

        try:
            content = wiki.find_article(int(args.path)).clean()

        except ValueError:
            with open(args.path, 'r') as f:
                content = f.read().decode('utf-8')

        c_list = lsh.find_candidates(content)
        print(c_list)

    # Main parser
    parser = argparse.ArgumentParser(
        description='Locality-Sensitive Hashing.'
    )

    # Common parser (common flags, used for inheritance)
    parse_common = argparse.ArgumentParser(add_help=False)
    parse_common.add_argument('-q', '--quiet', action='store_true',
                              help='omit output to stdout')

    subparsers = parser.add_subparsers(help='commands')

    # Parser for gen command
    parse_list = subparsers.add_parser('gen', help='generate signature matrix for documents', parents=[parse_common])
    parse_list.add_argument('-l', '--limit', type=int, metavar='N', help='limit to first N documents', default=max)
    parse_list.set_defaults(func=cmd_gen)

    # Parser for find command
    parse_find = subparsers.add_parser('lookup', help='find candidates for a single document', parents=[parse_common])
    parse_find.add_argument('path', type=str, metavar='FILE', help='path or ID to document')
    parse_find.set_defaults(func=cmd_lookup)

    args = parser.parse_args()

    if 'func' in args:
        args.func(args)
    else:
        parser.print_help()