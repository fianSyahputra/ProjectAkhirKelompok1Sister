from flask import Flask, Response
import json
from pymongo import MongoClient

app =  Flask(__name__)

client = MongoClient("localhost:27017")
db = client.distDB
list = db.dataSensor.find()
dict = {}
print list
for a in list:
    try:
		if(a["kelas"]!="-"):
			dict["node"].append({"id_node":a["id_node"], "temp":a["temp"], "hum":a["hum"], "pol":a["pol"], "kelas":a["kelas"]})
    except:
		if(a["kelas"]!="-"):
			dict={"node": [{"id_node":a["id_node"], "temp":a["temp"], "hum":a["hum"], "pol":a["pol"], "kelas":a["kelas"]}]}

@app.route('/node/', methods = ['GET'])
def semua():
    return Response(json.dumps(dict),mimetype='application/json')

@app.route('/node/<int:id_node>', methods = ['GET'])
def satu(id_node):
    return Response(json.dumps(dict[id_node]),mimetype='application/json')

app.run(host="0.0.0.0", debug = True, port = 5566)