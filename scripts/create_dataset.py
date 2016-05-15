# create dataset from edges file
from argparse import ArgumentParser
from itertools import combinations
import pandas as pd

from tweegraph.data import get_unique_nodes_from_file as unique_nodes


def get_pair_id(id_1, id_2):
    return str(id_1) + '_' + str(id_2)


def get_pair_labels(id_1, id_2):
    # create pairs of ids along with the feature labels / values
    global edges

    # labels
    if (id_1, id_2) in edges or (id_2, id_1) in edges:
        friends_or_not = 1
    else:
        friends_or_not = 0

    return (friends_or_not)


if __name__ == "__main__":
    parser = ArgumentParser(description='create_dataset')

    parser.add_argument('input_file',
                        metavar='input_file', type=str,
                        help='csv file that describes edges of the graph')

    args = parser.parse_args()
    file_name = args.input_file

    # taking the list of the unique nodes and creating all the pairs
    nodes = unique_nodes(file_name)
    pairs = list(combinations(nodes, 2))

    # turning edges to dict for fast look up
    edges = pd.read_csv(file_name, names=['id_1', 'id_2'])
    edges['label'] = 1
    edges = edges.set_index(['id_1', 'id_2']).to_dict()['label']

    data_set = pd.DataFrame(columns=['id_1', 'id_2', 'friends'])
    data_set['pair_id'] = map(lambda x: get_pair_id(x[0], x[1]), pairs)
    data_set['friends'] = map(lambda x: get_pair_labels(x[0], x[1]), pairs)

    data_set.to_csv('labeled_pairs.csv', names=True, index=False,
                    columns=['pair_id', 'friends'])
