import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt


links = pd.read_csv('links.csv', names=['follower', 'node'])

graph = nx.Graph()
# add nodes and edges
graph.add_edges_from(list(np.asarray(links.loc[1:, :])))

nx.draw(graph)
plt.show()
