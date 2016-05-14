import tweepy
from time import sleep


def create_api_instance(tokens):
    auth = tweepy.OAuthHandler(tokens['api_key'], tokens['api_secret'])
    auth.set_access_token(tokens['access'], tokens['access_secret'])
    api = tweepy.API(auth)
    return api


def request_handler(cursor, logger):
    """
    handle requests. If limit reached halt for 15 min
    """
    while True:
        try:
            yield cursor.next()
        except tweepy.TweepError as e:
            if 'code' in e.message[0] and e.message[0]['code'] == 88 or \
                    str(e.response) == '<Response [429]>':
                logger.info('Limit reached. Halting for 15 min')
                sleep(15 * 60)
                logger.info('Worker is active again')
            else:
                logger.warning(e.response)
                yield None


def request_data(query, node, size=None, logger=None):
    """request data from twitter

    Parameters
    ----------
        query  : tweepy.api instance query
        node   : id of node that is used for the query
        size   : amount of requested items (optional)
            used when specific amount of items is required.
        logger : logger instance to be used for logging events
    Returns
    -------
        data : a list with the requested data, if data are available

    Example
    -------
    >>> import tweepy 
    >>> import logging

    >>> auth = tweepy.OAuthHandler(api_key, api_secret)
    >>> auth.set_access_token(access, access_secret)
    >>> api = tweepy.API(auth)

    >>> logger = logging.getLogger('test_logger')

    >>> user = 1234125
    >>> user_timeline = request_data(api.user_timeline, node, logger=logger)
    """
    data = []
    handler = request_handler
    if not size:
        for page in handler(tweepy.Cursor(query, id=node).pages(), logger):
            if not page:
                return []
            data.extend(page)
            sleep(10)
    else:
        for item in handler(tweepy.Cursor(query, id=node).items(size), logger):
            if not item:
                return []
            data.append(item)
            sleep(10)
    return data
