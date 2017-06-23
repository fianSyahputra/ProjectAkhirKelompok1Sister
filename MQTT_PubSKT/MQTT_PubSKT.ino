#include <ESP8266WiFi.h>
#include <MQTTClient.h>

const char *ssid = "Yuzuru";
const char *pass = "daijobuka";

const byte numChars = 32;
char receivedChars[numChars]; 
boolean newData = false;

WiFiClient net;
MQTTClient client;

String incomming;
unsigned long lastMillis = 0;

void connect(); 

void setup() {
  Serial.begin(9600);
  WiFi.begin(ssid, pass);
  client.begin("ngehubx.online", net);
  connect();
}

void connect() {
  Serial.print("checking wifi...");
  while (WiFi.status() != WL_CONNECTED) {
    Serial.print(".");
    delay(1000);
  }
  Serial.print("\nconnecting...");
  while (!client.connect("arduino", "admintes", "admin123")) {
    Serial.print(".");
    delay(1000);
  }
  Serial.println("\nconnected!");
}

void loop() {
  client.loop();
  delay(10); 

  if(!client.connected()) { 
    connect();
  }
  if (Serial.available() > 0) {
    recvWithEndMarker();
    publishNewData();   
  }
}

void messageReceived(String topic, String payload, char * bytes, unsigned int length) {
  Serial.print("incoming: ");
  Serial.print(topic);
  Serial.print(" - ");
  Serial.print(payload);
  Serial.println();
}

void recvWithEndMarker() {
    static byte ndx = 0;
    char endMarker = '\n';
    char rc;
    
    while (Serial.available() > 0 && newData == false) {
        rc = Serial.read();

        if (rc != endMarker) {
            receivedChars[ndx] = rc;
            ndx++;
            if (ndx >= numChars) {
                ndx = numChars - 1;
            }
        }
        else {
            receivedChars[ndx] = '\0'; // terminate the string
            ndx = 0;
            newData = true;
        }
    }
}

void publishNewData() {
    if (newData == true) {
        Serial.println(receivedChars);
        client.publish("skt/node/1", receivedChars);
        newData = false;
    }
}
