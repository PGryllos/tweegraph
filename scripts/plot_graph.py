import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from argparse import ArgumentParser


if __name__ == '__main__':
    parser = ArgumentParser(description='draw network')

    parser.add_argument('input_file',
                        metavar='input_file', type=str,
                        help='csv file that describes edges of the graph')

    parser.add_argument('-d', '--directed', dest='directed', type=bool, default=False,
                        help='set to True for directed edges')

    args = parser.parse_args()
    file_name = args.input_file
    directed = args.directed

    links = pd.read_csv(file_name, names=['follower', 'node'])

    if directed:
        graph = nx.DiGraph()
    else:
        graph = nx.Graph()
    # add nodes and edges
    graph.add_edges_from(np.asarray(links))

    nx.draw(graph, with_labels = True)
    plt.show()
