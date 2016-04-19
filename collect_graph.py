import json
from time import sleep

from tweegraph.traverser import TwitterGraphTraverser
from pymongo import MongoClient


def number_of_users(db_name, collection_name):

    client = MongoClient()
    collection = client[db_name][collection_name]

    users = []

    for user in collection.find():
        users.append(user)
    return len(users)


if __name__ == "__main__":

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    starting_ids = [21447363,   # katy perry
                    27260086,   # justin bieber
                    813286,     # barack obama
                    25365536,   # kim kardashian
                    23375688,   # selena gomez
                    50393960,   # bill gates
                    31927467,]  # pitbull
    db_name = 'twitter_dinata_5'

    # create TwitterGraphTraverser instance
    traverser = TwitterGraphTraverser(breadth=400,
                                      graph_size=10000,
                                      starting_ids=starting_ids,
                                      credentials=credentials)
    # start exploring graph
    traverser.start()

    while True:
        if traverser.size() > 0:
            traverser.exportData()
            #print 'users in db: ', number_of_users(db_name, 'timelines')
            sleep(5)
