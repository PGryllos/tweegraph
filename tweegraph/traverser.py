import json
import Queue
import logging
import numpy as np
from time import sleep
from collections import defaultdict
from functools import wraps

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


def api_caller(logger_name):
    def api_caller_decorator(method):
        @wraps(method)
        def caller_wrapper(*args, **kwargs):
            logger = log_wrap(logger_name)
            kwargs['api'] = create_api_instance(kwargs['api'])
            logger.info('worker authenticated')
            return method(logger=logger, *args, **kwargs)
        return caller_wrapper
    return api_caller_decorator


class TwitterGraphTraverser:
    """
    TwitterGraphTraverser class. Implements traversing mechanism, in a BFS
    manner, taking advantage of multiple api keys if provided.
    """
    logger = log_wrap(log_name='twitter_traverser', console=True)

    def __init__(self, starting_ids, credentials, breadth, graph_size,
                 directions=['followers', 'following']):
        self.breadth = breadth
        self.graph_size = graph_size
        self.nodes_count = len(starting_ids)
        self.directions = directions
        self.explored_nodes = defaultdict(dict)
        self.new_nodes = Queue.Queue()
        self.credentials = credentials
        self.explored_lock = thread_lock()
        self.count_lock = thread_lock()

        for node in starting_ids:
            self.new_nodes.put(node)

    @api_caller(logger.name + '.graph_explorer')
    def explore_graph(self, api=None, logger=None):

        while True:
            explore = True
            followers = []
            following = []

            if self.get_size() >= self.graph_size:
                logger.info('terminating')
                return

            node = self.new_nodes.get(True)
            self.new_nodes.task_done()

            # check if node is already explored
            self.explored_lock.acquire()
            try:
                if node in self.explored_nodes:
                    explore = False
                else:
                    self.explored_nodes[node]['explored'] = True
            finally:
                self.explored_lock.release()

            # retrieve x followers of the node. x = breadth
            if explore and 'followers' in self.directions:
                followers = request_data(api.followers_ids, node,
                                         self.breadth, logger)
                for follower in followers:
                    self.new_nodes.put(follower)

            # retrieve x friends of the node. x = breadth
            if explore and 'following' in self.directions:
                following = request_data(api.friends_ids, node,
                                         self.breadth, logger)
                for friend in following:
                    self.new_nodes.put(friend)

            # lock used for thread safety with export_data method
            self.explored_lock.acquire()
            try:
                self.explored_nodes[node]['followers'] = followers
                self.explored_nodes[node]['following'] = following
            finally:
                self.explored_lock.release()

            self.count_lock.acquire()
            try:
                self.nodes_count += len(followers) + len(following)
            finally:
                self.count_lock.release()

    def export_data(self):
        """
        save relations in json format
        """
        self.explored_lock.acquire()
        try:
            with open('twitter_relations.json', 'w') as exported_data:
                json.dump(self.explored_nodes, exported_data)
        finally:
            self.explored_lock.release()

    def get_size(self):
        """
        return the number of collected nodes
        """
        self.count_lock.acquire()
        try:
            return self.nodes_count
        finally:
            self.count_lock.release()

    def start(self):
        """
        initiate graph traversing
        """
        # start as many crawlers as api keys
        for tokens in self.credentials:
            Thread(target=self.explore_graph, kwargs={'api': tokens}).start()
