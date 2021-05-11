import random
import uuid
from pymongo import MongoClient

mojo_db = MongoClient('mongodb://localhost:27017/').get_database('mojo')
full_users_col = mojo_db.get_collection('full_users')
users_attributes_col = mojo_db.get_collection('users_attributes')
scopes_coll = mojo_db.get_collection('scopes')

RANDOM_COUNT_SAMPLE = 10
TOTAL_USER = 100000
SCOPE_COUNT = 100
domains = ["joveo.com", "gmail.com", "aol.com", "mail.com", "mail.kz", "yahoo.com"]
letters = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]
applications = ['Mojo', 'MojoGo', 'ClientDashboard']
countries = ['USA', 'Germany', 'France', 'Europe', 'Asia', 'SouthAmerica']
levels = ['level-1', 'level-2', 'level-3']
allowedMetrics = ['applies', 'clicks', 'spend', 'cpc', 'cta', 'cpa']

import csv


def get_N_random_items():
    strs = []
    for i in range(RANDOM_COUNT_SAMPLE):
        strs.append(str(uuid.uuid1()))
    return strs


def get_one_random_domain():
    return domains[random.randint(0, len(domains) - 1)]


def get_one_random_name():
    return ''.join(random.choice(letters) for i in range(7))


def get_N_random_names():
    names = []
    for i in range(0, RANDOM_COUNT_SAMPLE):
        names.append(get_one_random_name())
    return names


agency_ids = get_N_random_names()
client_ids = get_N_random_items()
placement_ids = get_N_random_items()
users_data_file = open('./users_data.csv', 'w')
unique_users_data_file = open('./unique_users_data.csv', 'w')
scope_key_data_file = open('./scope_key_data.csv', 'w')

writer = csv.writer(users_data_file, delimiter=',')
writer_unique_users = csv.writer(unique_users_data_file, delimiter=',')
writer_scope_key = csv.writer(scope_key_data_file, delimiter=',')


def generate_random_users():
    emails = set()
    users = []
    scopeKeySet = set()
    for i in range(0, TOTAL_USER):
        user = {}
        user['emailId'] = get_one_random_name() + '@' + get_one_random_domain()
        if(user['emailId'] in emails):
            continue
        else :
            emails.add(user['emailId'])

        user['name'] = get_one_random_name()
        user['_id'] = str(uuid.uuid1())
        scopes = []

        for i in range(random.randint(0, SCOPE_COUNT)):
            unique_user = []
            user_row = [user['_id'], user['emailId'], user['name']]
            scope = {}
            unique_user.append(user['emailId'])
            unique_user.append(user['name'])

            application = applications[random.randint(0, len(applications) - 1)]
            scope['application'] = application
            if application == 'MojoGo':
                scope['instanceId'] = client_ids[random.randint(0, len(client_ids) - 1)]
                metadata = {}
                metadata['level'] = levels[random.randint(0, len(levels) - 1)]
                metadata['country'] = countries[random.randint(0, len(countries) - 1)]
                scope['metadata'] = metadata
            elif application == 'ClientDashboard':
                scope['instanceId'] = client_ids[random.randint(0, len(client_ids) - 1)]
                metadata = {}
                metadata['allowedMetrics'] = allowedMetrics[random.randint(0, len(allowedMetrics) - 1)]
                scope['metadata'] = metadata
            else:
                scope['instanceId'] = agency_ids[random.randint(0, len(agency_ids) - 1)]
                metadata = {}
                metadata['clientIds'] = [client_ids[random.randint(0, len(client_ids) - 1)],
                                      client_ids[random.randint(0, len(client_ids) - 1)],
                                      client_ids[random.randint(0, len(client_ids) - 1)]]
                metadata['placementIds'] = [placement_ids[random.randint(0, len(placement_ids) - 1)]]
                scope['metadata'] = metadata
            scopeKey = user['emailId'] + "_" + scope['application'] + '_' + scope['instanceId']
            if scopeKey in scopeKeySet:
                continue
            else:
                writer_scope_key.writerow([user['emailId'], scope['application'], scope['instanceId']])
                scopeKeySet.add(scopeKey)
            user_row.append(scope['application'])
            user_row.append(scope['instanceId'])
            user_row.append(str(scope['metadata']))
            scopes.append(scope)
            writer.writerow(user_row)
        writer_unique_users.writerow(unique_user)
        user['scopes'] = scopes
        users.append(user)
        full_users_col.insert_one(user)
    return users


if __name__ == '__main__':
    print(len(generate_random_users()))
