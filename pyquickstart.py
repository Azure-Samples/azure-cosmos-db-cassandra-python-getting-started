from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time

def PrintTable(rows):
    t = PrettyTable(['UserID', 'Name', 'City'])
    for r in rows:
        t.add_row([r.user_id, r.user_name, r.user_bcity])
    print t

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider)
session = cluster.connect()

print "\Creating Keyspace"
session.execute('CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'3\' }');
print "\Creating Table"
session.execute('CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)');

insert_data = session.prepare("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)")
batch = BatchStatement()
batch.add(insert_data, (1, 'VinodS', 'Dubai'))
batch.add(insert_data, (2, 'MohammedS', 'Toronto'))
batch.add(insert_data, (3, 'SiddeshV', 'Mumbai'))
batch.add(insert_data, (4, 'KirilG', 'Seattle'))
batch.add(insert_data, (5, 'GovindS', 'Belgaum'))
batch.add(insert_data, (6, 'CareyM', 'Seattle'))
batch.add(insert_data, (7, 'MatiasQ', 'Buenos Aires'))
batch.add(insert_data, (8, 'Samer', 'Seattle'))
batch.add(insert_data, (9, 'Mohit', 'Seattle'))
batch.add(insert_data, (10, 'Zarin', 'Seattle'))
batch.add(insert_data, (11, 'KannaP', 'Seattle'))
batch.add(insert_data, (12, 'Hema', 'Seattle' ))
batch.add(insert_data, (13, 'Madhan', 'Seattle'))
session.execute(batch)

print "\nSelecting All"
rows = session.execute('SELECT * FROM uprofile.user')
PrintTable(rows)

 
print "\nSelecting Id=1"
rows = session.execute('SELECT * FROM uprofile.user where user_id=1')
PrintTable(rows)

cluster.shutdown()