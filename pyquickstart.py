from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time
import ssl
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import *

def PrintTable(rows):
    t = PrettyTable(['UserID', 'Name', 'City'])
    for r in rows:
        t.add_row([r.user_id, r.user_name, r.user_bcity])
    print t

ssl_options = {
                  'ca_certs': 'path\to\cert',
                  'ssl_version': ssl.PROTOCOL_TLSv1_2
              }

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider, ssl_options=ssl_options
)
session = cluster.connect()

print "\nCreating Keyspace"
session.execute('CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }');
print "\nCreating Table"
session.execute('CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)');

insert_data = session.prepare("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)")
batch = BatchStatement()
batch.add(insert_data, (1, 'LyubovK', 'Dubai'))
batch.add(insert_data, (2, 'JiriK', 'Toronto'))
batch.add(insert_data, (3, 'IvanH', 'Mumbai'))
batch.add(insert_data, (4, 'YuliaT', 'Seattle'))
batch.add(insert_data, (5, 'IvanaV', 'Belgaum'))
batch.add(insert_data, (6, 'LiliyaB', 'Seattle'))
batch.add(insert_data, (7, 'JindrichH', 'Buenos Aires'))
batch.add(insert_data, (8, 'AdrianaS', 'Seattle'))
batch.add(insert_data, (9, 'JozefM', 'Seattle'))
batch.add(insert_data, (10, 'EmmaH', 'Seattle'))
batch.add(insert_data, (11, 'GrzegorzM', 'Seattle'))
batch.add(insert_data, (12, 'FryderykK', 'Seattle' ))
batch.add(insert_data, (13, 'DesislavaL', 'Seattle'))
session.execute(batch)

print "\nSelecting All"
rows = session.execute('SELECT * FROM uprofile.user')
PrintTable(rows)

 
print "\nSelecting Id=1"
rows = session.execute('SELECT * FROM uprofile.user where user_id=1')
PrintTable(rows)

cluster.shutdown()
