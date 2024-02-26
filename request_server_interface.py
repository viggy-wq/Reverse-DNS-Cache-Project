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

class SubscriberResolverInterface:
    def __init__(self, config):
        self.db_path = config['database']['path']
        self.queue_size = config['queue']['size']
        self.cleanup_items = set()
        self.client = paho.Client(client_id="subscriber_resolver_interface", callback_api_version=paho.CallbackAPIVersion.VERSION2)
        self.client.on_message = self.onMessage
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = self.conn.cursor()

        if self.client.connect("localhost", 1883, 60) != 0:
            print("Failed to connect to broker")
            sys.exit(-1)

        self.client.subscribe("server_availability")
        self.client.loop_forever()

    def serveQueue(self):
        while True:
            cursor = self.conn.cursor()
            cursor.execute("SELECT ip_address FROM ip_addresses")
            all_addresses = cursor.fetchall()
            address_list = [address[0] for address in all_addresses if address[0] not in self.cleanup_items]

            if len(address_list) >= self.queue_size:
                address_list = address_list[:self.queue_size]
                payload = ""
                for address in address_list:
                    payload = payload + address + " "
                self.client.publish("hostname_resolver_request", payload)
                print(f"Published: {address_list}")
                self.cleanup_items.update(address_list)
                # Break the loop once addresses are served
                break
            else:
                # Sleep for a bit before checking again if there are not enough addresses
                time.sleep(1)

    def onMessage(self, client, userdata, msg):
        if msg.topic == "server_availability":
            if msg.payload.decode() == "READY":
                self.serveQueue()

subscriber_resolver_interface = SubscriberResolverInterface(config)
    