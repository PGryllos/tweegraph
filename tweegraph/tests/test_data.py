import tweepy

from data import get_unique_nodes_from_dict as un_nodes_dict
from data import get_edges_from_dict as edges_dict
from data import get_mutual_following_edges as mutual_edges


test_dict = {
            '1': {
                    'followers': [2, 3, 45, 6, 88],
                    'following': [43, 65, 85, 123, 2]},
            '2': {
                    'followers': [1, 3, 4, 7, 88],
                    'following': [1, 343, 65, 85, 123]},
            '45': {
                    'followers': [3, 6, 88],
                    'following': [43, 65, 1, 123]}
            }

unique_nodes = [1, 2, 3, 4, 6, 7, 43, 45, 65, 85, 88, 123, 343]

edges = [(2, 1), (3, 1), (45, 1), (6, 1), (88, 1),
         (1, 43), (1, 65), (1, 85), (1, 123), (1,2),
         (1, 2), (3, 2), (4, 2), (7, 2), (88, 2),
         (2, 1), (2, 343), (2, 65), (2, 85), (2, 123),
         (3, 45), (6, 45), (88, 45),
         (45, 43), (45, 65), (45, 1), (45, 123)]

m_edges = [(2, 1), (1, 2), (1, 2), (2, 1)]


def test_unique_nodes():
    nodes = un_nodes_dict(test_dict)
    nodes.sort()
    assert nodes == unique_nodes


def test_edges():
    global test_dict
    all_edges = edges_dict(test_dict)
    assert all_edges == edges


def test_mutual_following():
    global test_dict
    global edges
    global m_edges
    list_of_mutual_edges = mutual_edges(test_dict, edges)
    assert list_of_mutual_edges == m_edges
