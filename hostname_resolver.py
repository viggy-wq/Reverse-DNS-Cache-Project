import sys
import dns.resolver, dns.reversename
import sqlite3
from datetime import datetime, timedelta
import json
import yaml
import select
import paho.mqtt.client as paho
import threading
import time

CREATE_CACHE = '''
            CREATE TABLE IF NOT EXISTS cache (
                ip_address TEXT PRIMARY KEY,
                hostname TEXT,
                timestamp DATETIME
            )
        '''
SELECT_TIMESTAMP = 'SELECT hostname, timestamp FROM cache WHERE ip_address = ?'
INSERT_INTO_CACHE = '''
            INSERT INTO cache (ip_address, hostname, timestamp) 
            VALUES (?, ?, ?)
            ON CONFLICT(ip_address) 
            DO UPDATE SET hostname = ?, timestamp = ?
        '''
CACHE_CLEANUP = 'DELETE FROM cache WHERE timestamp < ?'

class DnsCacheSync:
    def __init__(self, config):
        self.db_path = config['database']['path']
        self.dns_server = config['dns']['server']
        self.cache_timeout = timedelta(seconds=config['cache']['timeout'])
        self.json_dump_interval = config['jsonDump']['timeout'] 
        self.client = paho.Client(client_id="dnsCachesync", callback_api_version=paho.CallbackAPIVersion.VERSION2)
        self.client.on_message = self.onMessage
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA busy_timeout = 30000")
        cursor = self.conn.cursor()
        cursor.execute(CREATE_CACHE)
        self.conn.commit()

    def onMessage(self, client, userdata, msg):
        if msg.topic == "hostname_resolver_request":
            requests = msg.payload.decode().split(" ")

            for request in requests:
                self.processRequest(request)

            self.client.publish("server_availability", "READY", 0)
                
    def serverLoop(self):
        if self.client.connect("localhost", 1883, 60) != 0:
            print("Failed to connect to broker")
            sys.exit(-1)

        self.client.subscribe("hostname_resolver_request")
        self.client.publish("server_availability", "READY", 0)
        self.client.loop_forever()
    
    def routineLoop(self):
        cache_to_json_interval_seconds = self.json_dump_interval
        cleanup_cache_interval_seconds = 3600  # 1 hour in seconds

        cache_to_json_counter = 0
        cleanup_cache_counter = 0

        while True:
            start_time = time.time()

                # Perform cache to JSON dump if the interval has elapsed
            if cache_to_json_counter >= cache_to_json_interval_seconds:
                self.cacheToJson()
                cache_to_json_counter = 0  # Reset the counter

                # Perform cache cleanup if the interval has elapsed
            if cleanup_cache_counter >= cleanup_cache_interval_seconds:
                self.cleanupCache()
                cleanup_cache_counter = 0  # Reset the counter

                # Calculate elapsed time and update counters
            elapsed_time = time.time() - start_time
            cache_to_json_counter += elapsed_time
            cleanup_cache_counter += elapsed_time

    def getHost(self, ip):
        addr = dns.reversename.from_address(ip)
        my_resolver = dns.resolver.Resolver()
        my_resolver.nameservers = [self.dns_server]

        try:
            hostname = str(my_resolver.resolve(addr, 'PTR')[0])
        except dns.resolver.NXDOMAIN:
            hostname = "Hostname not found"
        except Exception as e:
            hostname = f"Error resolving hostname: {e}"
        return hostname

    def checkTimestamp(self, ip_address):
        cursor = self.conn.cursor()
        cursor.execute(SELECT_TIMESTAMP, (ip_address,))
        res = cursor.fetchone()

        if res:
            hostname, timestamp = res
            timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            if datetime.now() - timestamp < timedelta(hours=1):
                return hostname
        
        return None

    def updateCache(self, ip_address, hostname):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor = self.conn.cursor()
        cursor.execute(INSERT_INTO_CACHE, (ip_address, hostname, timestamp, hostname, timestamp))
        self.conn.commit()
        return hostname

    def cacheToJson(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM cache')
        rows = cursor.fetchall()
        cache_list = [{"ip_address": row[0], "hostname": row[1], "timestamp": row[2]} for row in rows]   
        print("Cache to JSON: ", json.dumps(cache_list, indent=4))
        self.client.publish("JSON", json.dumps(cache_list, indent=4), 0)

    def closeConnection(self):
        self.conn.close()
    
    def cleanupCache(self):
        current_time = datetime.now()
        threshold_time = current_time - self.cache_timeout

        # Convert the threshold time to string format for comparison in SQL query.
        threshold_time_str = threshold_time.strftime('%Y-%m-%d %H:%M:%S')

        cursor = self.conn.cursor()
        # Delete entries older than the threshold time.
        cursor.execute(CACHE_CLEANUP, (threshold_time_str,))
        self.conn.commit()

        print("Cache cleanup: Entries older than {} have been removed.".format(self.cache_timeout))

    def processRequest(self, ip_address):
        hostname = self.checkTimestamp(ip_address)
        if not hostname:
            hostname = self.getHost(ip_address)
            self.updateCache(ip_address, hostname)
        print(f"Processed: {ip_address} -> {hostname}")


def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main(config_path):
    config = load_config(config_path)
    db = DnsCacheSync(config)

    threading.Thread(target=db.routineLoop).start()
    db.serverLoop()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Missing config file")
        sys.exit(1)
    config_path = sys.argv[1]
    main(config_path)

#142.250.189.174 test ip address
#sfo03s24-in-f14.1e100.net. test hostname