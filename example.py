import json
from time import sleep

from tweegraph.traverser import TwitterGraphTraverser


if __name__ == "__main__":

    with open('credentials.json') as credentials_file:
        credentials = json.load(credentials_file)

    starting_id = 18937701

    # create TwitterGraphTraverser instance
    traverser = TwitterGraphTraverser(breadth=2,
                                      graph_size=140,
                                      central_id=starting_id,
                                      credentials=credentials)
    # start exploring graph
    traverser.start()

    while True:
        if traverser.size() > 0:
            traverser.exportData()
            sleep(5)
