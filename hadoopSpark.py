from bson.objectid import ObjectId
import json
import math
from pymongo import MongoClient
import subprocess
from pyspark import SparkContext
import time
import os 
sc=SparkContext.getOrCreate()
client = MongoClient("192.168.43.5:27017")
#ini nama DBnya
db = client['distDB']
#ini deklarasi collection/setara tabel
tabel=db['dataSensor']

def ambilData():
    parameter = training()
    print parameter[3]
    while True:

        f=open("/home/vagrant/dataNode.txt","w+")
        data=tabel.find({"kelas":"-"})
        for d in data:
	    print "AYY LMAO !!!!!!!!"
            inputString= str(d["id_node"])+","+str(d["temp"])+","+str(d["hum"])+","+str(d["pol"])+","+str(d["_id"])+"\n"
            f.write(inputString)
        f.close() 
        subprocess.call(["hadoop","fs","-put","/home/vagrant/dataNode.txt","/dataset"])
        
        dataUji = sc.textFile("/dataset/dataNode.txt")
	rdd = dataUji.map(lambda line: line.split(",")).map(lambda y: (float(y[0]),float(y[1]),float(y[2]),float(y[3]),str(y[4]))).collect()
        for data in rdd :
	    id = ObjectId(data[4])
	    x = [data[1],data[2],data[3]]
	    print "INI X1 ==  "+str(data[2])
	    indel=0
	    l = [None]*9
	    i = 0
	    while i<9 :
		pangkat = (((x[indel]-parameter[i]))**2/(2*(parameter[i+9]))**2)*(-1)
		bawah = math.sqrt(2*3.14)*parameter[i+9]
		l[i] = (1/bawah)*(2.71828183**pangkat)
		i = i+1
	    tinggi = l[0]*l[1]*l[2]
	    normal = l[3]*l[4]*l[5]
	    rendah = l[6]*l[7]*l[8]
	    max = tinggi
	    string = "tinggi"
	    if normal > max :
		max = normal
		string = "normal"
	    if rendah > max :
		max = rendah
	    	string = "rendah"
	    print "tinggi = "+str(tinggi)
	    print "normal = "+str(normal)
	    print "rendah = "+str(rendah)
	    print "Kelas = "+string
	    tabel.update({"_id":id},{"$set":{"kelas":string}})
	    

        subprocess.call(["hadoop","fs","-rm","/dataset/dataNode.txt"])
	print "======================================= SLEEP ========================================================"
	time.sleep(20)

def training():
    print "========================================================AKW;EOFALWI EFLAWEM;FALIWENF;LAMWEF;A ========================================================="
    dataTraining = sc.textFile("/dataset/dataTraining.txt")
    rdd = dataTraining.map(lambda line: line.split(",")).map(lambda y: (float(y[0]),float(y[1]),float(y[2]),str(y[3])))
    count = rdd.collect()
    rddsuhu = rdd.filter(lambda x: x[0] > 0).count()
    #rddsuhukelas = rdd.map(lambda x : (x[0],x[3]/countsByKey.value[x[0]])).collect()
    ratasuhutinggi = rdd.filter(lambda x: x[3]=="tinggi").map(lambda x : x[0]).mean()
    ratasuhusedang = rdd.filter(lambda x: x[3]=="normal").map(lambda x : x[0]).mean()
    ratasuhurendah = rdd.filter(lambda x: x[3]=="rendah").map(lambda x : x[0]).mean()

    ratahumtinggi = rdd.filter(lambda x: x[3]=="tinggi").map(lambda x : x[1]).mean()
    ratahumsedang = rdd.filter(lambda x: x[3]=="normal").map(lambda x : x[1]).mean()
    ratahumrendah = rdd.filter(lambda x: x[3]=="rendah").map(lambda x : x[1]).mean()

    ratapoltinggi = rdd.filter(lambda x: x[3]=="tinggi").map(lambda x : x[2]).mean()
    ratapolsedang = rdd.filter(lambda x: x[3]=="normal").map(lambda x : x[2]).mean()
    ratapolrendah = rdd.filter(lambda x: x[3]=="rendah").map(lambda x : x[2]).mean()

    stdsuhutinggi = rdd.filter(lambda x: x[3]=="tinggi").map(lambda x : x[0]).stdev()
    stdsuhusedang = rdd.filter(lambda x: x[3]=="normal").map(lambda x : x[0]).stdev()
    stdsuhurendah = rdd.filter(lambda x: x[3]=="rendah").map(lambda x : x[0]).stdev()

    stdhumtinggi = rdd.filter(lambda x: x[3]=="tinggi").map(lambda x : x[1]).stdev()
    stdhumsedang = rdd.filter(lambda x: x[3]=="normal").map(lambda x : x[1]).stdev()
    stdhumrendah = rdd.filter(lambda x: x[3]=="rendah").map(lambda x : x[1]).stdev()

    stdpoltinggi = rdd.filter(lambda x: x[3]=="tinggi").map(lambda x : x[2]).stdev()
    stdpolsedang = rdd.filter(lambda x: x[3]=="normal").map(lambda x : x[2]).stdev()
    stdpolrendah = rdd.filter(lambda x: x[3]=="rendah").map(lambda x : x[2]).stdev()

    data = [ratasuhutinggi,ratasuhusedang,ratasuhurendah,ratahumtinggi,ratahumsedang,ratahumrendah,ratapoltinggi,ratapolsedang,ratapolrendah,stdsuhutinggi,stdsuhusedang,stdsuhurendah,stdhumtinggi,stdhumsedang,stdhumrendah,stdpoltinggi,stdpolsedang,stdpolrendah]
    return data
data = [0]*5    
data[2] = "ttyy"
print data[1+1]    
ambilData()
