# helper functions to manipulate the data

import pandas as pd


def unique_nodes(file_name):
    """
    Given a file that describes the edges of a networks it returns all the
    unique nodes contained
    """
    edges = pd.read_csv(file_name, names=['node_1', 'node_2'])
    nodes = pd.concat([edges['node_1'], edges['node_2']])
    nodes = nodes.drop_duplicates()
    return list(nodes)
