import sys
import dns.resolver, dns.reversename
import sqlite3
from datetime import datetime, timedelta
import json
import yaml
import select
import paho.mqtt.client as paho

CREATE_IP_ADDRESS_LIST = '''CREATE TABLE IF NOT EXISTS ip_addresses
                 (ip_address TEXT PRIMARY KEY)'''

INSERT_IP_ADDRESS = "INSERT OR IGNORE INTO ip_addresses (ip_address) VALUES (?)"

class SubscriberDBSync:
    def __init__(self, config):
        self.db_path = config['database']['path']
        self.queue_size = config['queue']['size']
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute(CREATE_IP_ADDRESS_LIST)
        self.conn.commit()
    
    def insert(self, ip_address):
        cursor = self.conn.cursor
        cursor.execute(INSERT_IP_ADDRESS,(ip_address,))
        cursor.commit()
        
    def popQueue(self):
        cursor = self.conn.cursor
        cursor.execute("SELECT ip_address FROM ip_addresses LIMIT ?", (self.queue_size,))
        addresses = cursor.fetchall()
        print(addresses)

        # TODO read in self.queue_size number of ip_addresses from the sqlite db into a queue while deleting the entries placed in the queue from the db and return the queue
    

def onMessage(client, userdata, msg):
    if msg.topic == "ip_address":
        # TODO db.insert(address) possibly then sleep to prevent onMessage being stuck on ip_address topic

    if msg.topic == "availability":
        # TODO if DNSCacheSync ready for queue by posting ready in avaiability topic popQueue and publish queue contents to "ip_address_queue" topic

client = paho.Client(client_id="test", callback_api_version=paho.CallbackAPIVersion.VERSION2)
client.on_message = onMessage

if client.connect("localhost", 1883, 60) != 0:
    print("Failed to connect to broker")
    sys.exit(-1)

client.subscribe("test")

try:
    client.loop_forever()
except:
    print("Disconnecting")


client.disconnect()


