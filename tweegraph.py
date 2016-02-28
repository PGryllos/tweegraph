import tweepy
import time
import json


def limitHandler(cursor):
    """
    Generator that handles limit errors by pausing execution for some minutes
    """
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(3*60)


if __name__ == "__main__":
    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    # get credentials from credentials.json
    consumer_key = credentials['api_key']
    consumer_secret = credentials['api_secret']
    access_token = credentials['access_token']
    access_token_secret = credentials['access_token_secret']

    # authenticate
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create api instance
    api = tweepy.API(auth)

    # the node that the search will start from
    central_id = 2292454922
    for follower in limitHandler(tweepy.Cursor(
                                    api.followers_ids, id=central_id).pages()):
        print len(follower)
        time.sleep(10)
