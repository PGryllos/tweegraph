# helper functions to manipulate the data

import pandas as pd


def unique_nodes_from_file(file_name):
    """Returns all the unique nodes contained in a file that describes edges
    of a network

    Parameters
    ----------
        file_name : str
            csv file with two columns that both contain node ids
    Returns
    -------
        nodes : list of all the unique nodes
    """
    edges = pd.read_csv(file_name, names=['node_1', 'node_2'])
    nodes = pd.concat([edges['node_1'], edges['node_2']])
    nodes = nodes.drop_duplicates()
    return list(nodes)


def get_unique_nodes_from_dict(relations):
    """Produce list on unique nodes in the dictionary

    Parameters
    ----------
        relations : dictionary of dictionaries
            A dictionary that contains a dictionary with the keys 'following'
            and 'followers' for each key (id)
    Returns
    -------
        nodes : list of unique nodes in the dictionary
    """
    nodes = []
    for key in relations:
        nodes.extend(relations[key]['followers'])
        nodes.extend(relations[key]['following'])

    nodes = pd.DataFrame(nodes, columns=['node']).drop_duplicates()

    return list(nodes['node'])


def get_edges_from_dict(relations):
    """Produce the list edges described in the dictionary

    Parameters
    ----------
        relations : dictionary of dictionaries
            A dictionary that contains a dictionary with the keys 'following'
            and 'followers' for each key (id)
    Returns
    -------
        edges : list of tuples nodes in the form (follower, node)
    """
    edges = []
    for key in relations:
        rel_fol = [(f, int(key)) for f in relations[key]['followers']]
        rel_fri = [(int(key), f) for f in relations[key]['following']]
        edges.extend(rel_fol)
        edges.extend(rel_fri)

    return edges
