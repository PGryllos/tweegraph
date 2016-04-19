import json
import Queue
import logging
import pandas as pd
from time import sleep

from threading import Thread, Lock as thread_lock
from tweegraph.api import create_api_instance, request_data


def log_wrap(log_name, console=False, log_file=False, file_name='log.txt'):
    """
    logging module wrapper
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
            '[%(name)s - %(threadName)s - %(levelname)s] - %(message)s')
    if console:  # add console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if log_file:  # add file handler
        fh = logging.FileHandler(file_name)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


class TwitterGraphTraverser:
    """
    TwitterGraphTraverser class.

    Implements traversing mechanism, in a fixed-breadth manner, taking
    advantage of multiple api keys if provided.

    The server is implemented by find_nodes() which is responsible for adding
    new visited nodes for expansion. The crawling process is being handed by
    explore_graph() workers.
    """
    def __init__(self, starting_ids, credentials, breadth, graph_size):
        self.credentials = credentials
        self.breadth = breadth
        self.graph_size = graph_size
        self.starting_ids = starting_ids
        self.followers = Queue.Queue()
        self.following = Queue.Queue()
        self.found_nodes = Queue.Queue()
        self.explored_nodes = {}
        self.links = pd.DataFrame(columns=['nodeId', 'followerId'])
        self.dataLock = thread_lock()
        self.exploredLock = thread_lock()

        self.logger = log_wrap(log_name=self.__class__.__name__, console=True)

    def explore_graph(self, tokens):
        logger = log_wrap(self.logger.name + '.graph_explorer')

        api = create_api_instance(tokens)
        logger.info('worker authenticated')

        while True:
            explore = True
            followers = []
            following = []
            node = self.found_nodes.get(True)
            self.found_nodes.task_done()

            # check if node is already explored
            self.exploredLock.acquire()
            try:
                if node in self.explored_nodes:
                    explore = False
                else:
                    self.explored_nodes[node] = True
            finally:
                self.exploredLock.release()

            # retrieve x followers and x friends of the node. x = breadth / 2
            if explore:
                followers = request_data(api.followers_ids, node,
                                         self.breadth/2, logger)
            # avoid requests in case node is has set privacy on
            if followers:
                for follower in followers:
                    self.followers.put((follower, node))
                sleep(1)
                following = request_data(api.friends_ids, node,
                                         self.breadth/2, logger)
                for friend in following:
                    self.following.put((node, friend))
                sleep(1)

            # termination condition
            if self.size() >= self.graph_size:
                logger.info('terminating')
                return

    def find_nodes(self):
        """
        find new nodes for exploration
        """
        for node in self.starting_ids:
            self.found_nodes.put(node)

        while True:
            follower, node_1 = self.followers.get(True)
            self.followers.task_done()
            node_2, friend = self.following.get(True)
            self.following.task_done()
            self.dataLock.acquire()
            try:
                self.links.loc[len(self.links)] = node_1, follower
                self.links.loc[len(self.links)] = friend, node_2
            finally:
                self.dataLock.release()

            self.found_nodes.put(follower)
            self.found_nodes.put(friend)

    def exportData(self):
        """
        save links to file and clear dataframe
        """
        self.dataLock.acquire()
        try:
            self.links = self.links.astype(int)
            self.links.to_csv('links.csv', index=False, sep=',', header=False)
        finally:
            self.dataLock.release()

    def size(self):
        """
        return the size of the data in a thread safe manner
        """
        self.dataLock.acquire()
        try:
            return len(self.links)
        finally:
            self.dataLock.release()

    def start(self):
        """
        initiate graph traversing
        """
        # start node finder
        Thread(target=self.find_nodes).start()
        # start as many crawlers as api keys
        for tokens in self.credentials:
            Thread(target=self.explore_graph, args=(tokens,)).start()
