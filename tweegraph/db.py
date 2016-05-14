# MongoDB queries
from pymongo import MongoClient


def get_number_of_collections(db_name):
    """returns the number of collections stored in the db specified by
    db_name

    Parameters
    ----------
    db_name : str
        name of the database
    Returns
    -------
    count : number of collections in the db
    """
    db_client = MongoClient()
    return len(db_client[db_name].collection_names())


def get_tweets(db_name, user_id):
    """returns a list of the specified user's tweets. Collections in the db
    for each user must contain a list of the timelines under the key 'content'.

    Parameters
    ----------
    db_name : str
        name of the database
    user_id : id of twitter user

    Returns
    -------
    tweets : list of collected tweets for the specified user
    """
    tweets = []
    db_client = MongoClient()

    for timeline in db_client[db_name][user_id].find():
        for status in timeline['content']:
            tweets.append(status['text'])

    return tweets
