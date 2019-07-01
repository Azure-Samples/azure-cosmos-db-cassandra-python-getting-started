from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time
import ssl
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH


def PrintTable(rows):
    t = PrettyTable(['UserID', 'Name', 'City'])
    for r in rows:
        t.add_row([r.user_id, r.user_name, r.user_bcity])
    print t


ssl_opts = {
    'ca_certs': DEFAULT_CA_BUNDLE_PATH,
    'ssl_version': PROTOCOL_TLSv1_2,
}

if 'certpath' in cfg.config:
    ssl_opts['ca_certs'] = cfg.config['certpath']

auth_provider = PlainTextAuthProvider(
    username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port=cfg.config['port'], auth_provider=auth_provider, ssl_options=ssl_opts
                  )
session = cluster.connect()

print "\nCreating Keyspace"
session.execute(
    'CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }')
print "\nCreating Table"
session.execute(
    'CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)')

insert_data = session.prepare(
    "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)")
session.execute(insert_data, [1, 'Lybkov', 'Seattle'])
session.execute(insert_data, [2, 'Doniv', 'Dubai'])
session.execute(insert_data, [3, 'Keviv', 'Chennai'])
session.execute(insert_data, [4, 'Ehtevs', 'Pune'])
session.execute(insert_data, [5, 'Dnivog', 'Belgaum'])
session.execute(insert_data, [6, 'Ateegk', 'Narewadi'])
session.execute(insert_data, [7, 'KannabbuS', 'Yamkanmardi'])


print "\nSelecting All"
rows = session.execute('SELECT * FROM uprofile.user')
PrintTable(rows)


print "\nSelecting Id=1"
rows = session.execute('SELECT * FROM uprofile.user where user_id=1')
PrintTable(rows)

cluster.shutdown()
