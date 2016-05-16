import json
from argparse import ArgumentParser

from tweegraph.data import get_unique_nodes_from_file as unique_nodes
from tweegraph.traverser import crawl_timelines


if __name__ == "__main__":
    parser = ArgumentParser(description='collect_timelines')

    parser.add_argument('input_file',
                        metavar='input_file', type=str,
                        help='csv file that describes edges of the graph')
    parser.add_argument('-d', '--db_name', dest='db', type=str,
                        default='twitter_users')

    args = parser.parse_args()
    file_name = args.input_file

    db_name = args.db

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    nodes = unique_nodes(file_name)
    crawl_timelines(db_name, nodes, credentials)
