# script to collect a graph with 10000 twitter relations
import json
from time import sleep

from tweegraph.traverser import TwitterGraphTraverser

if __name__ == "__main__":

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    starting_ids = [21447363,    # katy perry
                    27260086,    # justin bieber
                    813286,      # barack obama
                    25365536,    # kim kardashian
                    23375688,    # selena gomez
                    50393960,    # bill gates
                    31927467,    # pitbull
                    2292454922]  # tsipras_eu
#
#   starting_ids = [2872661847, # Dennis Koutoudis
#                   124690469,  # Yanis Varoufakis
#                   337126547,  # Fay Skorda
#                   852894361,  # Paok against All
#                   585119509,  # httpmarinaa
#                   408566492,  # Panos Skourletis
#                   2292454922] # alexis tsipras

    # create TwitterGraphTraverser instance
    traverser = TwitterGraphTraverser(breadth=None,
                                      graph_size=400000,
                                      starting_ids=starting_ids,
                                      directions=['following'],
                                      credentials=credentials)
    # start exploring graph
    traverser.start()

    while True:
        if traverser.get_size() > 0:
            traverser.export_data()
            sleep(5)
