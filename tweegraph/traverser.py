import json
import tweepy
import Queue
import logging
import pandas as pd
from time import sleep
from threading import Thread, Lock as thread_lock, current_thread
from pymongo import MongoClient


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


def request_handler(cursor, logger):
    """
    handle requests. If limit reached halt for 15 min
    """
    while True:
        try:
            yield cursor.next()
        except tweepy.TweepError as e:
            if 'code' in e.message[0] and e.message[0]['code'] == 88:
                logger.info('Limit reached. Halting for 15 min')
                sleep(15 * 60)
            else:
                logger.warning(e)
                yield None


class TwitterGraphTraverser:
    """
    TwitterGraphTraverser class.

    Implements traversing mechanism taking advantage of multiple api keys if
    provided. At least two api keys needed. One for the retrieving user ids
    and one for collecting user data.

    The server is implemented by the nodeFinder method which is responsible
    for adding new visited nodes for expansion.

    The crawling process is being handed by graphExplorer workers while
    retrievingUserData workes are responsible for collecting user data that
    get stored using mongoDB.
    """
    def __init__(self, central_id, credentials, breadth, graph_size):
        self.credentials = credentials
        self.breadth = breadth
        self.graph_size = graph_size
        self.central_id = central_id
        self.node_count = 0
        self.followers = Queue.Queue()
        self.following = Queue.Queue()
        self.found_nodes = Queue.Queue()
        self.explored_nodes = {}
        self.links = pd.DataFrame(columns=['nodeId', 'followerId'])
        self.dataLock = thread_lock()
        self.exploredLock = thread_lock()

        self.logger = log_wrap(log_name=self.__class__.__name__, console=True)

    def graphExplorer(self, tokens):
        """
        visit node neighbours
        """
        logger = log_wrap(self.logger.name + '.graph_explorer')
        # authenticate worker and create api instance
        auth = tweepy.OAuthHandler(tokens['api_key'], tokens['api_secret'])
        auth.set_access_token(tokens['access'], tokens['access_secret'])
        api = tweepy.API(auth)

        logger.info('worker authenticated')
        while True:
            explore = True
            followers = []
            following = []
            node = self.found_nodes.get(True)
            self.found_nodes.task_done()

            # check if node is already explored
            self.exploredLock.acquire()
            self.node_count += self.breadth
            try:
                if self.node_count > self.graph_size:
                    explore = False
                if node in self.explored_nodes:
                    explore = False
                else:
                    self.explored_nodes[node] = True
            finally:
                self.exploredLock.release()

            # retrieve x followers and x friends of the node. x = breadth / 2
            if explore:
                for follower in request_handler(tweepy.Cursor(
                        api.followers_ids, id=node).items(self.breadth / 2),
                        logger):
                    if not follower:  # in case node is has set privacy on
                        explore = False
                        break
                    followers.append((follower, node))

                map(self.followers.put, followers)

                sleep(5)

            if explore:
                for friend in request_handler(tweepy.Cursor(
                        api.friends_ids, id=node).items(self.breadth / 2),
                        logger):
                    following.append((node, friend))

                map(self.following.put, following)

                sleep(5)

    def nodeFinder(self):
        """
        find new nodes for exploration
        """
        self.found_nodes.put(self.central_id)
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
        Thread(target=self.nodeFinder).start()
        # start as many crawlers as api keys
        for tokens in self.credentials:
            Thread(target=self.graphExplorer, args=(tokens,)).start()
