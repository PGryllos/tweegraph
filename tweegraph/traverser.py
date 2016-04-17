import json
import tweepy
import Queue
import pandas as pd
from time import sleep
from threading import Thread, Lock as thread_lock, current_thread
from pymongo import MongoClient


def request_handler(cursor):
    """
    handle requests. If limit reached halt for 15 min
    """
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            print 'Limit reached. Thread - ', current_thread(), e
            sleep(15 * 60)
        except tweepy.TweepError as e:
            print e


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
    def __init__(self, central_id, credentials, db_name, breadth, graph_size):
        self.credentials = credentials
        self.db = db_name
        self.breadth = breadth
        self.graph_size = graph_size
        self.central_id = central_id
        self.node_count = 0
        self.followers = Queue.Queue()
        self.following = Queue.Queue()
        self.foundNodes = Queue.Queue()
        self.exploredQueue = Queue.Queue()
        self.exploredNodes = {}
        self.links = pd.DataFrame(columns=['nodeId', 'followerId'])
        self.users = pd.DataFrame(columns=['nodeId'])
        self.dataLock = thread_lock()
        self.exploredLock = thread_lock()

    def graphExplorer(self, tokens):
        """
        visit node neighbours
        """
        # authenticate worker and create api instance
        auth = tweepy.OAuthHandler(tokens['api_key'], tokens['api_secret'])
        auth.set_access_token(tokens['access'], tokens['access_secret'])
        api = tweepy.API(auth)

        while True:
            explore = True
            followers = []
            following = []
            node = self.foundNodes.get(True)
            self.foundNodes.task_done()

            # check if node is already explored
            self.exploredLock.acquire()
            self.node_count += self.breadth
            try:
                if self.node_count > self.graph_size:
                    explore = False
                if node in self.exploredNodes:
                    explore = False
                else:
                    self.users.loc[len(self.users)] = node
                    self.exploredNodes[node] = True
                    self.exploredQueue.put(node)
            finally:
                self.exploredLock.release()

            # retrieve x followers and x friends of the node. x = breadth / 2
            if explore:
                for follower in request_handler(tweepy.Cursor(
                        api.followers_ids, id=node).items(self.breadth / 2)):
                    followers.append((follower, node))

                map(self.followers.put, followers)

                sleep(5)

                for friend in request_handler(tweepy.Cursor(
                        api.friends_ids, id=node).items(self.breadth / 2)):
                    following.append((node, friend))

                map(self.following.put, following)

                sleep(5)

    def retrieveUserData(self, tokens):
        """
        retrieve user data (timelines) and store them to the specified db
        """
        # authenticate worker and create api instance
        auth = tweepy.OAuthHandler(tokens['api_key'], tokens['api_secret'])
        auth.set_access_token(tokens['access'], tokens['access_secret'])
        api = tweepy.API(auth)

        db_client = MongoClient()

        while True:
            tweets = []
            node = self.exploredQueue.get(True)
            self.exploredQueue.task_done()

            for tweet in request_handler(tweepy.Cursor(
                    api.user_timeline, id=node).items(10)):
                tweets.append(tweet._json)

            timelines = db_client[self.db].timelines
            timelines.insert_one({'_id': node, 'content': tweets})

    def nodeFinder(self):
        """
        find new nodes for exploration
        """
        self.foundNodes.put(self.central_id)
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

            self.foundNodes.put(follower)
            self.foundNodes.put(friend)

    def exportData(self):
        """
        save links to file and clear dataframe
        """
        self.dataLock.acquire()
        try:
            self.links = self.links.astype(int)
            self.users = self.users.astype(int)
            self.links.to_csv('links.csv', index=False, sep=',', header=False)
            self.users.to_csv('users.csv', index=False, sep=',', header=False)
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
        Thread(target=self.nodeFinder).start()
        for tokens in self.credentials[0:1]:
            Thread(target=self.graphExplorer, args=(tokens,)).start()
        for tokens in self.credentials[1:3]:
            Thread(target=self.retrieveUserData, args=(tokens,)).start()

