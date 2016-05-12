import json

from threading import Thread
from pymongo import MongoClient
from time import sleep
from argparse import ArgumentParser

from tweegraph.api import create_api_instance, request_data
from tweegraph.traverser import api_caller
from tweegraph.data import unique_nodes
from tweegraph.db import get_number_of_users


@api_caller('retrieve_timelines')
def store_timelines(api=None, logger=None, collection, amount, user_list,
                    worker_id):
    for user in user_list:
        timeline = request_data(api.user_timeline, user, amount, logger)
        sleep(1)
        if timeline:
            timeline = [status._json for status in timeline]
            collection.insert_one({'_id': user, 'content': timeline})


if __name__ == "__main__":
    parser = ArgumentParser(description='collect_timelines')

    parser.add_argument('input_file',
                        metavar='input_file', type=str,
                        help='csv file that describes edges of the graph')
    parser.add_argument('-d', '--db_name', dest='db', type=str,
                        default='twitter_users')
    parser.add_argument('-c', '--collection_name', dest='col', type=str,
                        default='timelines')
    args = parser.parse_args()
    file_name = args.input_file

    db_name = args.db
    collection_name = args.col
    amount = 400  # amount of statuses to collect from each user

    logger = log_wrap('retrieve_timelines', console=True)

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    nodes = unique_nodes(file_name)

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
