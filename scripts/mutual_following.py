# given an edges file keep only the mutual following nodes
from argparse import ArgumentParser
import pandas as pd


def check_if_mutual(row):
    # check if both id_1 -> id_2 and id_2 -> id_1 relations exist
    id_1 = row[0]
    id_2 = row[1]

    if ((edges['id_1'] == id_2) & (edges['id_2'] == id_1)).any():
        return True
    else:
        return False


if __name__ == "__main__":
    parser = ArgumentParser(description='keep only mutual following nodes')

    parser.add_argument('input_file',
                        metavar='input_file', type=str,
                        help='csv file that describes edges of the graph')

    args = parser.parse_args()
    file_name = args.input_file

    edges = pd.read_csv(file_name, names=['id_1', 'id_2'])
    edges['mutual'] = True

    edges['mutual'] = edges[['id_1', 'id_2']].apply(check_if_mutual, 1)
    edges = edges[edges['mutual'] == True]

    edges.to_csv('mutual_following.csv', columns=['id_1', 'id_2'],
                 names=False, index=False)
