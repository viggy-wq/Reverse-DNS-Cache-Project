import sys
import dns.resolver, dns.reversename
import sqlite3
from datetime import datetime, timedelta
import json
import yaml
import select

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

class DnsCacheSync:
    def __init__(self, config):
        self.db_path = config['database']['path']
        self.dns_server = config['dns']['server']
        self.cache_timeout = timedelta(seconds=config['cache']['timeout'])
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute(CREATE_CACHE)
        self.conn.commit()
        
    def getHost(self, ip):
        addr = dns.reversename.from_address(ip)
        my_resolver = dns.resolver.Resolver()
        my_resolver.nameservers = [self.dns_server]
        hostname = str(my_resolver.resolve(addr, 'PTR')[0])
        
        if hostname:
            return hostname
        else:
            #return None
            print("Error resolving hostname")
            self.closeConnection()  
            sys.exit(1) # Handle case as needed

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
        return json.dumps(cache_list, indent=4)

    def closeConnection(self):
        self.conn.close()


def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main(config_path):
    config = load_config(config_path)
    dns_cache_sync = DnsCacheSync(config)

    last_dump_time = datetime.now()
    dump_interval = timedelta(minutes=2)  # Set to your desired interval

    try:
        while True:
            current_time = datetime.now()
            if current_time - last_dump_time >= dump_interval:
                # Time to dump the cache
                print(dns_cache_sync.cacheToJson())
                last_dump_time = current_time

            # Non-blocking input check
            if select.select([sys.stdin], [], [], 0.1)[0]:
                ip_address = sys.stdin.readline().strip()
                if ip_address == "exit":
                    print("Exiting...")
                    break
                if ip_address:
                    # Process the IP address here
                    hostname = dns_cache_sync.checkTimestamp(ip_address)
                    if not hostname:
                        hostname = dns_cache_sync.getHost(ip_address)
                        dns_cache_sync.updateCache(ip_address, hostname)
                    print(f"Processed: {ip_address} -> {hostname}")
    finally:
        dns_cache_sync.closeConnection()



def resolver(config_path):
    ip_address = '142.250.189.174' 
    config = load_config(config_path)
    db = DnsCacheSync(config)
    hostname = db.checkTimestamp(ip_address)
    if hostname:
        db.updateCache(ip_address, hostname)
        print(hostname)
    else:
        db.updateCache(ip_address, db.getHost(ip_address))
        print(db.cacheToJson())
    db.closeConnection()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Missing config file")
        sys.exit(1)
    config_path = sys.argv[1]
    main(config_path)

