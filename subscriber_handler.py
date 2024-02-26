import sys
import dns.resolver, dns.reversename
import sqlite3
from datetime import datetime, timedelta
import json
import yaml
import select
import paho.mqtt.client as paho
import time

CREATE_IP_ADDRESS_LIST = '''CREATE TABLE IF NOT EXISTS ip_addresses
                 (ip_address TEXT PRIMARY KEY)'''

INSERT_IP_ADDRESS = "INSERT OR IGNORE INTO ip_addresses (ip_address) VALUES (?)"

config = {
    'database': {'path': 'subscriber_db.sqlite'}
}

class SubscriberDBSync:
    def __init__(self, config):
        self.db_path = config['database']['path']
        self.client = paho.Client(client_id="test", callback_api_version=paho.CallbackAPIVersion.VERSION2)
        self.client.on_message = self.onMessage
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = self.conn.cursor()
        cursor.execute(CREATE_IP_ADDRESS_LIST)
        self.conn.commit()

        if self.client.connect("localhost", 1883, 60) != 0:
            print("Failed to connect to broker")
            sys.exit(-1)

        self.client.subscribe("ip_address")
        self.client.loop_start()
    
    def insert(self, ip_address):
        cursor = self.conn.cursor()
        cursor.execute(INSERT_IP_ADDRESS,(ip_address,))
        self.conn.commit()
            
    def onMessage(self, client, userdata, msg):
        if msg.topic == "ip_address":
            ip_address = msg.payload.decode()
            self.insert(ip_address)
            print(f"Inserted IP address: {ip_address}")

    def print_db(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM ip_addresses')
        for row in cursor.fetchall():
            print(row)

subscriber_db_sync = SubscriberDBSync(config)
time.sleep(10)
subscriber_db_sync.print_db()

