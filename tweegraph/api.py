import tweepy
from time import sleep


def create_api_instance(tokens):
    """return authenticated tweepy api instance. In order to do that
    you need to provide a dictionary which values are the four tokens
    provided by apps.twitter when registering an app.

    Parameters
    ----------
    tokens : dict{'api_key': <api_key>, 'api_secret': <api_secret>,
                  'access': <access>, 'access_secret': <secret_access>}
    """
    auth = tweepy.OAuthHandler(tokens['api_key'], tokens['api_secret'])
    auth.set_access_token(tokens['access'], tokens['access_secret'])
    if 'proxy' in tokens:
        api = tweepy.API(auth, proxy=tokens['proxy'])
    else:
        api = tweepy.API(auth)
    return api


def request_handler(cursor, logger):
    """
    handle requests. If limit reached halt for 15 min
    """
    retries = 0
    while True:
        try:
            yield cursor.next()
        except tweepy.TweepError as e:
            LOG_MSG = 'exploring node: ' + str(cursor.__dict__['kargs']['id'])
            if 'code' in e.message[0] and e.message[0]['code'] == 88 or \
                    str(e.response) == '<Response [429]>':
                logger.info('Limit reached. Halting for 15 min. ' + LOG_MSG)
                sleep(15 * 60)
                logger.info('Worker is active again')
            else:
                if e[0][:22] == 'Failed to send request':
                    if retries == 100:
                        logger.warning('Too many connection retries' + LOG_MSG)
                        retries = 0
                    retries += 1

                    sleep(0.5)
                    continue
                else:
                    logger.warning(e)
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
