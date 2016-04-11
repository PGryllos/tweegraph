import tweepy
import Queue
import json
import pandas as pd
from time import sleep
from threading import Thread, Lock as thread_lock, current_thread

# TODO - make use a multiple access tokens for working around api limits
# TODO - enable NetworkX or Giphy like functionality
# TODO - add information to nodes


class TwitterGraphTraverser:
    """
    TwitterGraphTraverser class. Implements traversing mechanism in breadth
    first kind-of manner. The specified breadth will define the breadth that
    will be explored.
    """
    def __init__(self, central_id, credentials, breadth=250, graph_size=2000):
        self.credentials = credentials
        self.breadth = breadth
        self.graph_size = graph_size
        self.node_count = 0
        self.followers = Queue.Queue()
        self.following = Queue.Queue()
        self.foundNodes = Queue.Queue()
        self.exploredNodes = {}
        self.links = pd.DataFrame(columns=['nodeId', 'followerId'])
        self.users = pd.DataFrame(columns=['nodeId'])
        self.dataLock = thread_lock()
        self.exploredLock = thread_lock()
        self.central_id = central_id

    def graphExplorer(self, tokens):
        """
        Exploring new nodes
        """
        # authenticate worker
        auth = tweepy.OAuthHandler(tokens['api_key'], tokens['api_secret'])
        auth.set_access_token(tokens['access'], tokens['access_secret'])

        # create api instance
        api = tweepy.API(auth)
        while True:
            explore = True
            followers = []
            following = []
            node = self.foundNodes.get(True)
            self.foundNodes.task_done()
            # check if node is already explored
            self.exploredLock.acquire()
            self.node_count += 2 * self.breadth
            try:
                if self.node_count > self.graph_size:
                    explore = False
                if node in self.exploredNodes:
                    explore = False
                else:
                    self.users.loc[len(self.users)] = node  # extend dataset
                    self.exploredNodes[node] = True
            finally:
                self.exploredLock.release()
            # explore node
            if explore:
                for follower in self.limitHandler(tweepy.Cursor(
                        api.followers_ids, id=node).items(self.breadth)):
                    followers.append((follower, node))
                map(self.followers.put, followers)
                sleep(5)
                for friend in self.limitHandler(tweepy.Cursor(
                        api.friends_ids, id=node).items(self.breadth)):
                    following.append((node, friend))
                map(self.following.put, following)
                sleep(5)

    def nodeFinder(self):
        """
        Finding new nodes for exploration
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
        Save links to file and clear dataframe
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
        Returns the size of the data in a thread safe manner
        """
        self.dataLock.acquire()
        try:
            return len(self.links)
        finally:
            self.dataLock.release()

    def start(self):
        """
        Initiates graph traversing
        """
        Thread(target=self.nodeFinder).start()
        for tokens in self.credentials:
            Thread(target=self.graphExplorer, args=(tokens,)).start()

    @staticmethod
    def limitHandler(cursor):
        """
        Generator that handles limit errors by pausing execution for some
        minutes
        """
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                print 'Limit reached. Thread - ', current_thread()
                sleep(15 * 60)


if __name__ == "__main__":

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    starting_id = 18937701

    # create TwitterGraphTraverser instance
    traverser = TwitterGraphTraverser(breadth=400,
                                      central_id=starting_id,
                                      credentials=credentials)
    # start exploring graph
    traverser.start()

    while True:
        if traverser.size() >= 500:
            traverser.exportData()
        else:
            print 'Relationships ready to be exported: ', traverser.size()
            sleep(8)
