import json

from threading import Thread
from pymongo import MongoClient
from time import sleep
from argparse import ArgumentParser

from tweegraph.api import request_data
from tweegraph.traverser import log_wrap, api_caller
from tweegraph.data import get_unique_nodes_from_file as unique_nodes


@api_caller('collect_timelines.retriever')
def get_timelines(db, user_list, api=None, logger=None):
    for user in user_list:
        timeline = request_data(api.user_timeline, user, 1, logger)
        sleep(1)
        if timeline:
            timeline = [status._json for status in timeline]
            db[user].insert_one({'_id': user, 'content': timeline})


if __name__ == "__main__":
    parser = ArgumentParser(description='collect_timelines')

    parser.add_argument('input_file',
                        metavar='input_file', type=str,
                        help='csv file that describes edges of the graph')
    parser.add_argument('-d', '--db_name', dest='db', type=str,
                        default='twitter_users')

    args = parser.parse_args()
    file_name = args.input_file

    db_name = args.db

    logger = log_wrap('collect_timelines', console=True)

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    nodes = unique_nodes(file_name)

    db_client = MongoClient()
    db = db_client[db_name]

    chunk = int(len(nodes) / len(credentials))
    a = 0
    # spawn a crawler for each of the equally divided pieces of the user list
    for idx, tokens in enumerate(credentials[:-1]):
        user_list = nodes[idx * chunk: chunk * (idx+1)]
        Thread(target=get_timelines,
               args=(db, user_list), kwargs={'api': tokens}).start()

    user_list = nodes[(idx+1) * chunk:]
    Thread(target=get_timelines,
           args=(db, user_list), kwargs={'api': credentials[-1]}).start()
