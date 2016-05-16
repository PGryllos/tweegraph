# MongoDB queries
from collections import defaultdict
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


def create_user_topic_affiliation_dict(db_name):
    """returns the user-topic affilication dictionary proposed by Yuan et al.
    in their study 'Exploiting Sentiment Homophily for Link Prediction'

    Parameters
    ----------
    db_name : str
        name of the database. Database is supposed to contain a collection for
        each user. Each collection is supposed to contain a list of retrieved
        statuses under the key 'content'

    Returns
    -------
    user_topic_dict : dict in the for dict{user_id: {topic: sentiment_score}}
    """
    user_topic_dict = defaultdict(dict)

    db_client = MongoClient()

    for user in db_client[db_name].collection_names():
        for timeline in db_client[db_name][user].find():
            for status in timeline['content']:
                for hashtag in status['entities']['hashtags']:
                    user_topic_dict[user][hashtag['text']] = [0, 1, 0]

    return user_topic_dict


def create_topic_user_dict(db_name):
    """returns the a topic to list of users dictionary

    Parameters
    ----------
    db_name : str
        name of the database. Database is supposed to contain a collection for
        each user. Each collection is supposed to contain a list of retrieved
        statuses under the key 'content'

    Returns
    -------
    topic_user_dict : dict in the for dict{topic: [user_1, user_2, ...]}
    """
    topic_user_dict = defaultdict(list)

    db_client = MongoClient()

    for user in db_client[db_name].collection_names():
        for timeline in db_client[db_name][user].find():
            hashtags = []
            for status in timeline['content']:
                for hashtag in status['entities']['hashtags']:
                    hashtags.append(hashtag['text'])
            for hashtag in set(hashtags):
                topic_user_dict[hashtag].append(user)

    return topic_user_dict
