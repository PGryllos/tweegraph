import json
import pandas as pd

from threading import Thread
from pymongo import MongoClient
from time import sleep
from tweegraph.api import create_api_instance, request_data
from tweegraph.traverser import log_wrap


def get_number_of_users(collection):
    users = []
    for user in collection.find():
        users.append(user)
    return len(users)


def store_timelines(tokens, collection, amount, user_list, worker_id):
    logger = log_wrap('retrieve_timelines.' + str(worker_id))

    api = create_api_instance(tokens)
    logger.info('worker authenticated')

    for user in user_list:
        timeline = request_data(api.user_timeline, user, amount, logger)
        if timeline:
            timeline = [status._json for status in timeline]
            collection.insert_one({'_id': user, 'content': timeline})


if __name__ == "__main__":

    db_name = 'Twitter_users'
    collection_name = 'timelines'
    amount = 400

    logger = log_wrap('retrieve_timelines', console=True)

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    nodes = pd.read_csv('links.csv', names=['node1', 'node2'])

    # keep unique nodes
    nodes = pd.concat([nodes['node1'], nodes['node2']])
    nodes = nodes.drop_duplicates()
    nodes = list(nodes)

    db_client = MongoClient()
    timelines = db_client[db_name][collection_name]

    chunk = int(len(nodes) / len(credentials))

    # spawn a crawler for each of the equally divided pieces of the user list
    for idx, tokens in enumerate(credentials):
        user_list = nodes[idx * chunk: chunk * (idx+1)]
        Thread(target=store_timelines,
               args=(tokens, timelines, amount, user_list, idx)).start()

    while True:
        logger.info('#users in db ' + str(get_number_of_users(timelines)))
        sleep(2 * 60)
