import sys
import dns.resolver, dns.reversename
import sqlite3
from datetime import datetime, timedelta
import json
import yaml
import select
import paho.mqtt.client as paho
import time

config = {
    'database': {'path': 'subscriber_db.sqlite'},
    'queue': {'size': 5}
}

SERVE_QUEUE = "SELECT id, ip_address FROM ip_addresses ORDER BY id ASC LIMIT ?"
DELETE_QUEUE = "DELETE FROM ip_addresses WHERE id IN (SELECT id FROM ip_addresses ORDER BY id ASC LIMIT ?)"

class RequestResolverInterface:
    def __init__(self, config):
        self.db_path = config['database']['path']
        self.queue_size = config['queue']['size']
        self.cleanup_items = set()
        self.client = paho.Client(client_id="subscriber_resolver_interface", callback_api_version=paho.CallbackAPIVersion.VERSION2)
        self.client.on_message = self.onMessage
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA busy_timeout = 30000")
        cursor = self.conn.cursor()

        if self.client.connect("localhost", 1883, 60) != 0:
            print("Failed to connect to broker")
            sys.exit(-1)

        self.client.subscribe("server_availability")
        self.client.loop_forever()

    def serveQueue(self):
        while True:
            cursor = self.conn.cursor()
            cursor.execute(SERVE_QUEUE, (self.queue_size,))
            all_addresses = cursor.fetchall()

            if len(all_addresses) >= self.queue_size:
                address_list = [address[1] for address in all_addresses]
                payload = " ".join(address_list)
                # Publish the addresses
                self.client.publish("hostname_resolver_request", payload)
                cursor.execute(DELETE_QUEUE, (self.queue_size,))
                self.conn.commit()

                print(f"Published and deleted: {address_list}")
                # Break the loop once addresses are served
                break
            else:
                # Sleep for a bit before checking again if there are not enough addresses
                time.sleep(.5)
        
        time.sleep(.5)

    def onMessage(self, client, userdata, msg):
        if msg.topic == "server_availability":
            if msg.payload.decode() == "READY":
                self.serveQueue()

request_handler = RequestResolverInterface(config)
    