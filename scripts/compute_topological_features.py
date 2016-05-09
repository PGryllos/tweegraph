import numpy as np
import pandas as pd
import networkx as nx
from argparse import ArgumentParser


def get_shortest_distance(pair_id):
    global graph
    id_1 = int(pair_id.split('_')[0])
    id_2 = int(pair_id.split('_')[1])
    try:
        return len(nx.shortest_path(graph, source=id_1, target=id_2)) - 1
    except:
        return -1


def get_clustering_index(pair_id):
    global graph
    id_1 = int(pair_id.split('_')[0])
    id_2 = int(pair_id.split('_')[1])
    
    return nx.clustering(graph, id_1) + nx.clustering(graph, id_2)


def get_neighbors(pair_id):
    global graph
    id_1 = int(pair_id.split('_')[0])
    id_2 = int(pair_id.split('_')[1])
    
    return len(nx.neighbors(graph, id_1)) + len(nx.neighbors(graph, id_2))


if __name__ == '__main__':
    parser = ArgumentParser(description='compute shortest paths')

    parser.add_argument('edges_file',
                        metavar='edges_file', type=str,
                        help='csv file that describes edges of the graph')

    parser.add_argument('data_set',
                        metavar='data_set', type=str,
                        help='csv file that describes all pairs of nodes')

    args = parser.parse_args()
    edges = args.edges_file
    data_set = args.data_set

    edges = pd.read_csv(edges, names=['node_1', 'node_2'])
    data_set = pd.read_csv(data_set)
    
    graph = nx.Graph()
    graph.add_edges_from(np.asarray(edges))

    data_set['path'] = data_set['pair_id'].apply(get_shortest_distance, 1)
    data_set['clustering'] = data_set['pair_id'].apply(get_clustering_index, 1)
    data_set['neighbors'] = data_set['pair_id'].apply(get_neighbors, 1)
        
    print data_set

    data_set.to_csv('topological_features.csv', index=False)
