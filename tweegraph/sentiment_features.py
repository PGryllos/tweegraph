"""construction of sentiment features given a set of user ids
"""

# Author: Prokopios Gryllos
# gryllosprokopis@gmail.com

from __future__ import division
from sys import maxint
import math
from tweegraph.db import get_user_topic_affiliation_dict
from tweegraph.db import get_topic_user_dict
from tweegraph.db import determine_polarity

_USER_TOPIC_DICT = {}
_TOPIC_USER_DICT = {}


def set_user_topic_dict(user_topic_dict):
    """used to set the user topic affiliation dict that is going to be used
    for producing the correlations. Must always be set before using the
    metrics.
    """
    global _USER_TOPIC_DICT
    _USER_TOPIC_DICT = dict(user_topic_dict)


def set_topic_user_dict(topic_user_dict):
    """used to set the topic user affiliation dict that is going to be used
    for producing the correlations. Must always be set before using the
    metrics.
    """
    global _TOPIC_USER_DICT
    _TOPIC_USER_DICT = dict(topic_user_dict)


def svo_distance(user_id, hashtag, a=0.45, b=0.45, c=0.1):
    """retuns the sentiment volume objectivity metric
    """
    # sentiment
    count_pos, count_neg, count_neut = _USER_TOPIC_DICT[user_id][hashtag][3:6]
    sentiment = count_pos - count_neg / (count_pos + count_neg)
    sentiment = 1 / (1 + 10**-sentiment)  # normalization

    hashtag_count = count_pos + count_neg + count_neut

    # objectivity
    objectivity = count_neut / hashtag_count

    # volume
    total_count = 0
    for hashtag in _USER_TOPIC_DICT[user_id]:
        total_count += sum(_USER_TOPIC_DICT[user_id][hashtag][3:6])
    volume = hashtag_count / total_count
    
    return a * sentiment + b * volume + c * objectivity
    

def get_common_hashtags(user_id_1, user_id_2):
    """returns a list of the hashtags the two users have in common. If size is
    set to True instead of the list only the size of the list is returned
    """
    common_hashtags = list(set(_USER_TOPIC_DICT[user_id_2]) & \
                           set(_USER_TOPIC_DICT[user_id_1]))
    return common_hashtags


def get_sentiment_agreement(user_id_1, user_id_2,
                            exclude=[], common_hashtags=[]):
    """returns the amount of common hashtags for which the two user expressed
    the same sentiment. The parameter exclude is a list of sentiments that
    should not be taken into consideration when summing up the results.
    For example if exclude equals [1, 0] then sentiment agreement will be
    accumulated over topics affiliated only with negative sentiment (-1).
    """
    agreement_score = 0
    if not common_hashtags:
        common_hashtags = get_common_hashtags(user_id_1, user_id_2)

    for hashtag in common_hashtags:
        pol_1 = determine_polarity((_USER_TOPIC_DICT[user_id_1][hashtag][:3]))
        pol_2 = determine_polarity((_USER_TOPIC_DICT[user_id_2][hashtag][:3]))

        if pol_1 == pol_2 and pol_1 not in exclude:
            agreement_score += 1

    return agreement_score


def get_sentiment_disagreement(user_id_1, user_id_2, common_hashtags=[]):
    """returns the amount of common hashtags for which the two users expressed
    opposite sentiment. Excluding hashtags for which the users expressed
    objective opinion.
    """
    disagreement_score = 0
    if not common_hashtags:
        common_hashtags = get_common_hashtags(user_id_1, user_id_2)

    for hashtag in common_hashtags:
        pol_1 = determine_polarity((_USER_TOPIC_DICT[user_id_1][hashtag][:3]))
        pol_2 = determine_polarity((_USER_TOPIC_DICT[user_id_2][hashtag][:3]))

        if pol_1 and pol_1 == -pol_2:
            disagreement_score += 1

    return disagreement_score


def get_sentiment_alignment_coef(user_id_1, user_id_2, common_hashtags=[]):
    """returns the ratio of sentiment agreement (excluding objective opinions)
    over the amount of common hashtags between the two users. count is the
    amount of common hashtags, if not provided its calculated.
    """
    if not common_hashtags:
        common_hashtags = get_common_hashtags(user_id_1, user_id_2)

    agreement = get_sentiment_agreement(user_id_1, user_id_2, exclude=[0],
                                        common_hashtags=common_hashtags)

    count = len(common_hashtags)

    if count:
        return agreement / count
    else:
        return -1


def get_sentiment_misalignment_coef(user_id_1, user_id_2, common_hashtags=[]):
    """returns the ratio of sentiment disagreement over the amount of common
    hashtags
    """
    if not common_hashtags:
        common_hashtags = get_common_hashtags(user_id_1, user_id_2)

    disagreement = get_sentiment_disagreement(user_id_1, user_id_2,
                                              common_hashtags=common_hashtags)
    count = len(common_hashtags)

    if count:
        return disagreement / count
    else:
        return -1


def get_sentiment_rarest(user_id_1, user_id_2, common_hashtags=[]):
    """returns the adoption of the rarest common hashtag excluding hashtags
    for which the users expressed objective opinion. Returns -1 if no common
    hashtag with polar agreement is found.
    """
    adoption = maxint
    if not common_hashtags:
        common_hashtags = get_common_hashtags(user_id_1, user_id_2)

    for hashtag in common_hashtags:
        pol_1 = determine_polarity((_USER_TOPIC_DICT[user_id_1][hashtag][:3]))
        pol_2 = determine_polarity((_USER_TOPIC_DICT[user_id_2][hashtag][:3]))

        if pol_1 and pol_1 == pol_2:
            adoption = min(adoption, len(_TOPIC_USER_DICT[hashtag]))

    if adoption != maxint:
        return adoption
    else:
        return -1


def get_sentiment_adamic_adar(user_id_1, user_id_2, common_hashtags=[]):
    """returns the sum of tha adamic adar distances for all common hashtags for
    which the users share the same opinion
    """
    adamic_adar_score = 0
    if not common_hashtags:
        common_hashtags = get_common_hashtags(user_id_1, user_id_2)

    for hashtag in common_hashtags:
        pol_1 = determine_polarity((_USER_TOPIC_DICT[user_id_1][hashtag][:3]))
        pol_2 = determine_polarity((_USER_TOPIC_DICT[user_id_2][hashtag][:3]))

        if pol_1 == pol_2:
            adamic_adar_score += 1 / math.log(len(_TOPIC_USER_DICT[hashtag]))

    return adamic_adar_score


def get_sentiment_inverse(user_id_1, user_id_2, common_hashtags=[]):
    """returns the sum of the inverses of the adoptions for each common hashtag
    for which the users share the same opinion
    """
    inverse = 0
    if not common_hashtags:
        common_hashtags = get_common_hashtags(user_id_1, user_id_2)

    for hashtag in common_hashtags:
        pol_1 = determine_polarity((_USER_TOPIC_DICT[user_id_1][hashtag][:3]))
        pol_2 = determine_polarity((_USER_TOPIC_DICT[user_id_2][hashtag][:3]))

        if pol_1 == pol_2:
            inverse += 1 / len(_TOPIC_USER_DICT[hashtag])

    return inverse


def get_size_of_common_hashtags(user_id_1, user_id_2, common_hashtags=[]):
    """returns the mean size of common hashtags by summing up the adoption of
    all hashtags for which the users share the same opinion and diving that
    by the sentiment agreement of the users
    """
    mean_size = 0
    if not common_hashtags:
        common_hashtags = get_common_hashtags(user_id_1, user_id_2)

    sentiment_agreement = get_sentiment_agreement(
            user_id_1, user_id_2, common_hashtags=common_hashtags)

    for hashtag in common_hashtags:
        pol_1 = determine_polarity((_USER_TOPIC_DICT[user_id_1][hashtag][:3]))
        pol_2 = determine_polarity((_USER_TOPIC_DICT[user_id_2][hashtag][:3]))

        if pol_1 == pol_2:
            mean_size += len(_TOPIC_USER_DICT[hashtag])

    if sentiment_agreement:
        return mean_size / sentiment_agreement
    else:
        return -1


