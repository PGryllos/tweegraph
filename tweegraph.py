import tweepy
import Queue
import json
import threading

import pandas as pd

from time import sleep


class TwitterGraphTraverser():
    """
    TwitterGraphTraverser class. Implements traversing mechanism in breadth
    first kind-of manner. The specified breadth will define the breadth that
    will be explored.
    """
    def __init__(self, breadth=None, central_id=None, api=None):
        self.api = api
        self.breadth = breadth

        self.followers = Queue.Queue()
        self.following = Queue.Queue()
        self.foundNodes = Queue.Queue()
        self.exploredNodes = {}
        self.relationships = pd.DataFrame(columns=['nodeId', 'followerId'])
        self.dataLock = threading.Lock()

        self.foundNodes.put(central_id)

    def graphExplorer(self):
        """
        Exploring new nodes
        """
        while True:
            node = self.foundNodes.get(True)
            self.foundNodes.task_done()

            if node not in self.exploredNodes:
                for follower in self.limitHandler(tweepy.Cursor(
                        self.api.followers_ids, id=node).items(self.breadth)):
                    self.followers.put((follower, node))
                sleep(5)
                for friend in self.limitHandler(tweepy.Cursor(
                        self.api.friends_ids, id=node).items(self.breadth)):
                    self.following.put((node, friend))
                sleep(5)
                self.exploredNodes[node] = True

    def nodeFinder(self):
        """
        Finding new nodes for exploration
        """
        while True:
            follower, node = self.followers.get(True)
            self.followers.task_done()
            self.relationships.loc[len(self.relationships)] = node, follower

            node, friend = self.following.get(True)
            self.following.task_done()
            self.dataLock.acquire()
            try:
                self.relationships.loc[len(self.relationships)] = friend, node
            finally:
                self.dataLock.release()

            self.foundNodes.put(follower)
            self.foundNodes.put(friend)

    def exportData(self):
        """
        Save relationships to file and clear dataframe
        """
        self.dataLock.acquire()
        try:
            self.relationships.to_csv(
                    'relationships.csv', mode='a', index=False, sep=',')
            self.relationships = self.relationships.drop(
                                                self.relationships.index)
        finally:
            self.dataLock.release()

    def size(self):
        """
        Returns the size of the data in a thread safe manner
        """
        self.dataLock.acquire()
        try:
            return len(self.relationships)
        finally:
            self.dataLock.release()

    def start(self):
        """
        Initiates graph traversing
        """
        threading.Thread(target=self.graphExplorer).start()
        threading.Thread(target=self.nodeFinder).start()

    def limitHandler(self, cursor):
        """
        Generator that handles limit errors by pausing execution for some
        minutes
        """
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                print 'Limit reached. stand by'
                sleep(15 * 60)


if __name__ == "__main__":

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    # get credentials from credentials.json
    consumer_key = credentials['api_key']
    consumer_secret = credentials['api_secret']
    access_token = credentials['access_token']
    access_token_secret = credentials['access_token_secret']

    # authenticate
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create api instance
    tweeApi = tweepy.API(auth)

    # the node that the search will start from
    starting_id = 18937701

    # create TwitterGraphTraverser instance
    traverser = TwitterGraphTraverser(breadth=400,
                                      central_id=starting_id,
                                      api=tweeApi)
    # start exploring graph
    traverser.start()

    while True:
        if traverser.size() >= 2000:
            traverser.exportData()
        else:
            sleep(5)
