import paho.mqtt.client as mqtt
from pymongo import MongoClient

#connect to DB
client = MongoClient("localhost:27017")
db = client.distDB

# inisiasi object mqtt
mqttc = mqtt.Client("sub20", clean_session=True)

# Buat koneksi ke broker
mqttc.username_pw_set("admintes", "admin123")
mqttc.connect("ngehubx.online", 1883)

#kalo konek tampilkan Connected
def on_connect(mqttc, obj, flags, rc):
	print("Connected")

def on_message(mqttc, obj, msg):
	print("Topik " + msg.topic + " Payload : " + msg.payload + " QoS " + str(msg.qos))

	#olah data dari broker
	data = msg.payload
	data = data.split(" ")
	id = float(data[0])
	temp = float(data[1])
	hum = float(data[2])
	pol = float(data[3])

	#Input Data ke DB
	db.dataSensor.insert_one({'id_node' : id, 'temp': temp, 'hum': hum, 'pol':pol, 'kelas':'-'})

# Set callback function
mqttc.on_connect = on_connect
mqttc.on_message = on_message
# Subscribe ke topik tertentu
mqttc.subscribe("skt/node/1")
mqttc.subscribe("skt/node/2")
mqttc.subscribe("skt/node/3")

# Loop supaya subscribernya tidak berhenti
mqttc.loop_forever()