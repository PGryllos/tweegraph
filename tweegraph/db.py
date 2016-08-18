# MongoDB queries
from collections import defaultdict
from pymongo import MongoClient
from pymongo.errors import DocumentTooLarge as DocumentTooLargeError
from pymongo.errors import WriteError

from tweegraph.sentiment_analyzer import get_tweet_score, determine_polarity


def store_timeline(db_name, user_id, timeline):
    """stores the timeline as a list of json formated statuses in
    db -> db_name[user_id] under the key 'content'

    db_name  : name of the database
    user_id  : twitter id of user
    timeline : user timeline entity as returned by the twitter api
    """
    db_client = MongoClient()
    timeline = [status._json for status in timeline]

    if timeline:
        while True:
            try:
                db_client[db_name][str(user_id)].insert_one(
                        {'_id': user_id, 'content': timeline})
                break
            except DocumentTooLargeError as e:
                timeline = timeline[:-1]
            except WriteError as e:
                timeline = timeline[:-1]


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


def get_user_topic_affiliation_dict(db_name):
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
    user_topic : dict in the for dict{user_id: {topic: sentiment_score}}
    """
    user_topic = defaultdict(dict)

    db_client = MongoClient()

    for user in db_client[db_name].collection_names():
        timeline = db_client[db_name][user].find().next()
        for status in timeline['content']:
            tweet_score = get_tweet_score(status['text'])
            polarity = determine_polarity(tweet_score)
            for topic in status['entities']['hashtags']:
                topic = topic['text'].lower()
                if not topic in user_topic[user]:
                    # tweet score accompanied by the pos neg and obj count
                    user_topic[user][topic] = [tweet_score, 0, 0, 0]
                else:
                    previous_score = user_topic[user][topic][0]

                    new_score = tuple(sum(i) for i in zip(tweet_score,
                                                          previous_score))
                    user_topic[user][topic][0] = new_score
                if polarity == 1:
                    user_topic[user][topic][1] += 1
                elif polarity == -1:
                    user_topic[user][topic][2] += 1
                else:
                    user_topic[user][topic][3] += 1
    # average scores
    for user in user_topic:
        for topic, scores in user_topic[user].iteritems():
            freq = sum(scores[1:])
            if freq > 1:
                #print scores
                pos = scores[0][0] / freq
                neg = scores[0][1] / freq

                user_topic[user][topic] = (pos, neg, round(1 - (pos + neg), 7),
                                           scores[1], scores[2], scores[3])
            else:
                user_topic[user][topic] = (scores[0][0], scores[0][1],
                                           scores[0][2], scores[1], scores[2],
                                           scores[3])

    return dict(user_topic)


def get_topic_user_dict(db_name):
    """returns a topic to list of users dictionary

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
        timeline = db_client[db_name][user].find().next()
        hashtags = []
        for status in timeline['content']:
            for hashtag in status['entities']['hashtags']:
                hashtags.append(hashtag['text'])
        for hashtag in set(hashtags):
            topic_user_dict[hashtag.lower()].append(user)

    return dict(topic_user_dict)
