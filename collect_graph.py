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

    # create TwitterGraphTraverser instance
    traverser = TwitterGraphTraverser(breadth=None,
                                      graph_size=2500,
                                      starting_ids=starting_ids,
                                      directions=['following'],
                                      credentials=credentials)
    # start exploring graph
    traverser.start()

    while True:
        if traverser.size() > 0:
            traverser.exportData()
            sleep(5)
