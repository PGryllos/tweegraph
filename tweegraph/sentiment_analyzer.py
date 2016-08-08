"""Basic Sentiment analysis using TextBlob's PatternAnalyzer and a manually
annotated dictionary of a set of emojis
"""

# Author: Prokopios Gryllos
# gryllosprokopis@gmail.com

from __future__ import division
import re
import os
import pandas as pd

from textblob.sentiments import PatternAnalyzer


def _get_emoticons_dict():
    """Returns a dictionary that maps unicode codes of emojis to sentiment
    score of the form (positivity, negativy, objectivity)
    """
    emoticons_sent_dict = {}

    path = os.path.dirname(os.path.realpath(__file__))
    emot_sent = pd.read_csv(path + '/' + 'emoticons_sentiments.csv')

    emot_sent = emot_sent[['unicode_code',
                           'pos_score',
                           'neg_score',
                           'obj_score']]

    emot_sent = emot_sent.set_index('unicode_code').to_dict()
    emot_sent = [emot_sent['pos_score'],
                 emot_sent['neg_score'],
                 emot_sent['obj_score']]

    for key in emot_sent[0]:
        emoticons_sent_dict[key] = tuple(score[key] for score in emot_sent)

    return emoticons_sent_dict


_EMOTICONS_DICT = _get_emoticons_dict()
_SENTIMENT_ANALYZER = PatternAnalyzer()


def get_tweet_score(tweet=None):
    """Returns sentiment score of tweet by averaging the text score as decided
    by the PatternAnalyzer with the average score of the detected emojis
    """
    text_score = [0, 0]  # pos_score, neg_score
    emot_score = [0, 0]  # pos_score, neg_score
    emot_sent_scores = []

    # find sentiment for text using pattern analyzer from textblob
    polarity = _SENTIMENT_ANALYZER.analyze(tweet).polarity
    if polarity > 0:
        text_score[0] = polarity
    else:
        text_score[1] = abs(polarity)

    # find emoticons
    emoticons = re.findall(u'[\U0001f601-\U0001f64f]', tweet)

    # retrieve sentiment scores of emoticons using annotation dict
    for emot_code in emoticons:
        emot_code = emot_code.encode('unicode_escape')
        if emot_code in _EMOTICONS_DICT:
            emot_sent_scores.append(_EMOTICONS_DICT[emot_code])
    # taking the sum of pos and neg
    for score in emot_sent_scores:
        emot_score[0] += score[0]
        emot_score[1] += score[1]

    if emot_sent_scores:
        # averaging
        emot_score[0] = emot_score[0] / len(emot_sent_scores)
        emot_score[1] = emot_score[1] / len(emot_sent_scores)

        pos_score = (emot_score[0] + text_score[0]) / 2
        neg_score = (emot_score[1] + text_score[1]) / 2
    else:
        pos_score = text_score[0]
        neg_score = text_score[1]


    tweet_score = (pos_score, neg_score, round(1 - (pos_score + neg_score), 7))

    return tweet_score
