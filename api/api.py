import flask
import psycopg2
import pymongo
import csv
from Constants import *

app = flask.Flask(__name__)
app.config["DEBUG"] = True

conn = psycopg2.connect("dbname='joveo' user='postgres' host='localhost' password='Newuser1234**'")
cur = conn.cursor()

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mojo"]
mycol = mydb["full_users"]

email_app_inst = []


def fetch_scope_key():
    with open('./../scope_key_data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if len(row) == 0:
                continue
            emailId = row[0]
            application = row[1]
            instanceId = row[2]
            scope = {}
            scope['application'] = application
            scope['instanceId'] = instanceId
            scope['email'] = emailId
            email_app_inst.append(scope)


fetch_scope_key()

totScopes = len(email_app_inst)


@app.route('/', methods=['GET'])
def home():
    return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for distant reading of science fiction novels.</p>"


@app.route('/postgres/user', methods=['GET'])
def getPostgresData():
    scope = email_app_inst[Constant.postgresCnt % totScopes]
    cur.execute("""SELECT * FROM scopes s ,joveo_users u WHERE s.email = '{0}' AND s.application = 
    '{1}' AND s.instanceId = '{2}' AND s.email=u.email""".format(scope['email'], scope['application'],
                                                                 scope['instanceId']))
    x = cur.fetchall()
    resp = {}
    if (len(x) != 0):
        resp = {'id': x[0][0],
                'name': x[0][7],
                'email': x[0][1],
                'application': x[0][2],
                'instance': x[0][3],
                'metadata': x[0][4]}

    Constant.postgresCnt += 1
    return str(resp)


@app.route('/mongo/user', methods=['GET'])
def getMongoData():
    scope = email_app_inst[Constant.mongoCnt % totScopes]
    myquery = {"emailId": scope["email"]}
    myprojection = {
        "scopes": {"$elemMatch": {"application": scope["application"], "instanceId": scope["instanceId"]}},
        "name": True,
        "emailId": True
    }
    mydoc = mycol.find(myquery, myprojection)
    Constant.mongoCnt += 1
    res = None
    for x in mydoc:
        res = x
    return str(res)

@app.route('/mongo/scope', methods=['GET'])
def getMongoScope():
    scope = email_app_inst[Constant.mongoCnt % totScopes]
    myquery = {"emailId": scope["email"]}
    myprojection = {
        "scopes": {"$elemMatch": {"application": scope["application"], "instanceId": scope["instanceId"]}},
        "name": True
    }
    mydoc = mycol.find(myquery, myprojection)
    Constant.mongoCnt += 1
    res = None
    for x in mydoc:
        res = x
    return str(res)

@app.route('/postgres/scope', methods=['GET'])
def getPostgresScope():
    scope = email_app_inst[Constant.postgresCnt % totScopes]
    cur.execute("""SELECT * FROM scopes s WHERE s.email = '{0}' AND s.application = 
    '{1}' AND s.instanceId = '{2}'""".format(scope['email'], scope['application'],
                                                                 scope['instanceId']))
    x = cur.fetchall()
    resp = {}
    if (len(x) != 0):
        resp = {'id': x[0][0],
                'email': x[0][1],
                'application': x[0][2],
                'instance': x[0][3],
                'metadata': x[0][4]}

    Constant.postgresCnt += 1
    return str(resp)

@app.route('/hello', methods=['GET'])
def hello():
    return "hello"


app.run(host='0.0.0.0', port=80)