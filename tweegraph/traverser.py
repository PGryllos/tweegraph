import json
import Queue
import logging
import numpy as np
from time import sleep

from threading import Thread, Lock as thread_lock
from tweegraph.api import create_api_instance, request_data


def log_wrap(log_name, console=False, log_file=False, file_name='log.txt'):
    """
    logging module wrapper

    Parameters
    ----------

    log_name  : str
        name to use for creating logger
    console   : bool
        logging to console
    log_file  : bool
        logging to specified log file
    file_name : str
        name of the file to use for logging if log_file is set to True

    Returns
    -------

    logger : logger object
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
            '[%(asctime)s - %(name)s - %(threadName)s - '
            '%(levelname)s] - %(message)s')
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
    def __init__(self, starting_ids, credentials, breadth, graph_size,
                 directions=['followers', 'following']):
        self.breadth = breadth
        self.graph_size = graph_size
        self.directions = directions
        self.starting_ids = starting_ids
        self.links = []
        self.explored_nodes = {}
        self.connections = Queue.Queue()
        self.found_nodes = Queue.Queue()
        self.credentials = credentials
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

            # termination condition
            if self.size() >= self.graph_size:
                logger.info('terminating')
                return

            # retrieve x followers of the node. x = breadth
            if explore and 'followers' in self.directions:
                followers = request_data(api.followers_ids, node,
                                         self.breadth, logger)
                # avoid spending requests in case node has set privacy on
                if not followers:
                    continue
                for follower in followers:
                    self.connections.put((follower, node))

            # retrieve x friends of the node. x = breadth
            if explore and 'following' in self.directions:
                following = request_data(api.friends_ids, node,
                                         self.breadth, logger)
                if not following:
                    continue
                for friend in following:
                    self.connections.put((node, friend))

    def find_nodes(self):
        """
        find new nodes for exploration
        """
        for node in self.starting_ids:
            self.found_nodes.put(node)

        while True:
            follower, node = self.connections.get(True)
            self.connections.task_done()

            # lock used for thread safety between find_nodes and export_data
            self.dataLock.acquire()
            try:
                self.links.append((follower, node))
            finally:
                self.dataLock.release()

            self.found_nodes.put(follower)
            self.found_nodes.put(node)

    def exportData(self):
        """
        save links to file
        """
        self.dataLock.acquire()
        try:
            np.savetxt('links.csv', self.links, fmt='%i')
        finally:
            self.dataLock.release()

    def size(self):
        """
        return the size of the data
        """
        return len(self.links)

    def start(self):
        """
        initiate graph traversing
        """
        # start node finder
        Thread(target=self.find_nodes).start()
        # start as many crawlers as api keys
        for tokens in self.credentials:
            Thread(target=self.explore_graph, args=(tokens,)).start()
