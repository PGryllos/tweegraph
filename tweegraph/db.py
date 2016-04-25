# MongoDB queries


def get_number_of_users(collection):
    users = []
    for user in collection.find():
        users.append(user)
    return len(users)
