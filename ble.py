from bluepy import btle
import random
import time
import json
from paho.mqtt import client as mqtt_client
broker = '103.1.238.175'
port = 1885
topic = "metter"
# Generate a Client ID with the publish prefix.
client_id = f'publish-pi4-{random.randint(0, 1000)}'
username = 'test'
password = 'testadmin'
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client
client = connect_mqtt()
class MyDelegate(btle.DefaultDelegate):
    def __init__(self):
        btle.DefaultDelegate.__init__(self)
        # ... initialise here

    def handleNotification(self, cHandle, data):
        voltage = (data[0] << 8 | data[1]) / 10.00
        current = (data[2] << 8 | data[3] | data[4] << 24 | data[5] << 16) / 1000.00
        power = (data[6] << 8 | data[7] | data[8] << 24 | data[9] << 16) / 10.00
        energy = (data[10] << 8 | data[11] | data[12] << 24 | data[13] << 16) / 1000.000
        freq = (data[14] << 8 | data[15]) / 10.0
        pf = (data[16] << 8 | data[17]) / 100.00
        device_id = "5c:1a:6f:b6:42:10"
        data_str = {
            "Device ID": device_id,
            "Voltage": voltage,
            "Current": current,
            "Power": power,
            "Energy": energy,
            "Frequency": freq,
            "Power Factor": pf
        }
        json_data = json.dumps(data_str)
        print("---------------------------")
        print(json_data)
        result = client.publish(topic, json_data)
        status = result[0]
        if(status == 0):
            print("Success")
        else:
            print("Failed")

# Initialisation  -------

p = btle.Peripheral("c8:f0:9e:9a:3e:7a")
p.withDelegate( MyDelegate() )

# Setup to turn notifications on, e.g.
svc = p.getServiceByUUID("4fafc201-1fb5-459e-8fcc-c5c9c331914b")
ch = svc.getCharacteristics("beb5483e-36e1-4688-b7f5-ea07361b26a8")[0]


setup_data = b"\x01\00"
p.writeCharacteristic(ch.valHandle+1, setup_data)

# Main loop --------
while True:
    if p.waitForNotifications(2.0):
        continue