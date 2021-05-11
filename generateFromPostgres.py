import csv
import pprint
import psycopg2
import json
import ast
import traceback

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

    try:
        cur.execute("CREATE TABLE joveo_users (	id serial PRIMARY KEY, email VARCHAR (50) UNIQUE,display_name VARCHAR (50));")
    except:
        print("joveo_users already exists")


    try:
        cur.execute("CREATE TABLE scopes (	id serial PRIMARY KEY,	email VARCHAR (50) ,name VARCHAR (50),application VARCHAR (50),instanceId VARCHAR (50),metadata JSONB,	CONSTRAINT fk_email FOREIGN KEY(email) REFERENCES joveo_users(email))")
    except:
        print("scopes already exists")

    cnt=0
    for usr in unique_usr_data:
        cnt+=1
        try:
            cur.execute(
                'INSERT INTO "joveo_users" (email, display_name) VALUES (%s, %s)',
                (usr['email'], usr["name"]))
        except Exception as e:
            tb = traceback.format_exc()
            print(usr)
            print(tb)
            raise e

    print("cnt = "+str(cnt))

    for usrStr in per_user_data:
        scopes = per_user_data[usrStr]
        for scope in scopes:
            data = json.dumps(scope["metadata"])
            cur.execute(
                'INSERT INTO scopes (email, application, instanceId, metadata) VALUES (%s, %s, %s,%s)',
                (scope['email'], scope["application"], scope["instanceId"], Json(ast.literal_eval(scope['metadata']))))

    conn.commit()
    cur.close()
    conn.close()
