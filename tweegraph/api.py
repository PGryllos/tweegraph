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


def request_data(query, node, amount, logger):
    items = []
    handler = request_handler
    for item in handler(tweepy.Cursor(query, id=node).items(amount), logger):
        if not item:
            return None
        items.append(item)
    return items
