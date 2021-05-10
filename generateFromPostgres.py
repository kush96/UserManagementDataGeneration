import csv
import pprint
import psycopg2
import json
import ast

from psycopg2._json import Json

per_user_data = {}
unique_usr_data = []
email_app_inst = []

# // 5432
def fetch_users_from_file():
    with open('./users_data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if len(row) == 0:
                continue
            _id = row[0]
            emailId = row[1]
            name = row[2]
            application = row[3]
            instanceId = row[4]
            metadata = row[5]
            scope = {}
            scope['application'] = application
            scope['instanceId'] = instanceId
            scope['metadata'] = metadata
            scope['email'] = emailId
            scope['name'] = name

            key = _id + '_' + name + '_' + emailId
            if key in per_user_data:
                per_user_data[key].append(scope)
            else:
                per_user_data[key] = []


def fetch_unique_users_from_file():
    with open('./unique_users_data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if len(row) == 0:
                continue
            emailId = row[0]
            name = row[1]
            user = {}
            user['email'] = emailId
            user['name'] = name
            unique_usr_data.append(user)




if __name__ == '__main__':
    fetch_users_from_file()
    fetch_unique_users_from_file()
    conn = psycopg2.connect("dbname='joveo' user='postgres' host='localhost' password='Newuser1234**'")
    cur = conn.cursor()

    for usr in unique_usr_data:
        cur.execute(
            'INSERT INTO public.joveo_users (email, display_name) VALUES (%s, %s)',
            (usr['email'], usr["name"]))

    for usrStr in per_user_data:
        print(usrStr)
        scopes = per_user_data[usrStr]
        for scope in scopes:
            data = json.dumps(scope["metadata"])
            cur.execute(
                'INSERT INTO public.scopes (email, application, instanceId, metadata) VALUES (%s, %s, %s,%s)',
                (scope['email'], scope["application"], scope["instanceId"], Json(ast.literal_eval(scope['metadata']))))

    pprint.pprint(per_user_data)
    conn.commit()
    cur.close()
    conn.close()
