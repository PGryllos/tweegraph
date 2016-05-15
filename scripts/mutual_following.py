# given an edges file keep only the mutual following nodes
import json
import pandas as pd
from argparse import ArgumentParser

from tweegraph.data import get_mutual_following_edges as mutual_edges


if __name__ == "__main__":
    parser = ArgumentParser(description='keep only mutual following nodes')

    parser.add_argument('input_file',
                        metavar='input_file', type=str,
                        help='csv file that describes edges of the graph')

    args = parser.parse_args()
    file_name = args.input_file

    with open(file_name) as json_file:
        relations_dict = json.load(json_file)

    m_edges = mutual_edges(relations_dict)
    m_edges = pd.DataFrame(m_edges, columns=['follower', 'node'])
    m_edges = m_edges.drop_duplicates()

    m_edges.to_csv('mutual_following.csv', columns=['follower', 'node'],
                   names=False, index=False)
