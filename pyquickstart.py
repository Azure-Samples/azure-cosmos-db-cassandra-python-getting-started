"""
Sample quickstart guide for Azure Cosmos DB Cassandra API
"""
from asyncio.log import logger
import json
from cassandra.cluster import Cluster
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from cassandra.auth import PlainTextAuthProvider
from prettytable import PrettyTable
import config as cfg

f = open('sample/error_code.json')
data = json.load(f)

def execute_command(query, values=None):
    try:
        session.execute(query, values)
    except Exception as exception:
        if str(exception.error_code) in data:
            print(data[str(exception.error_code)])
        else:
            logger.error(exception.error_code, exception.message)

def PrintTable(rows):
    t = PrettyTable(['UserID', 'Name', 'City'])
    for r in rows:
        t.add_row([r.user_id, r.user_name, r.user_bcity])
    print (t)

def getTableCount(rows):
    D = {}
    for i in range(len(rows)):
        if rows[i][0] in D:
            D[rows[i][0]].append(rows[i][1])
        else:
            D[rows[i][0]]= []
            D[rows[i][0]].append(rows[i][1])


    s = PrettyTable(['keyspace_name', 'Num_of_Tables'])
    for new_k, new_val in D.items():
        s.add_row([new_k, len([item for item in new_val if item])])
    print(s)

#<authenticateAndConnect>
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.verify_mode = CERT_NONE
auth_provider = PlainTextAuthProvider(username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider,ssl_context=ssl_context)
session = cluster.connect()
#</authenticateAndConnect>

#<createKeyspace>
print ("\nCreating Keyspace")
execute_command('CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }');
#</createKeyspace>

#<createTable>
print ("\nCreating Table")
execute_command('CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)');
#</createTable>

#<insertData>
execute_command("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [1,'Lybkov','Seattle'])
execute_command("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [2,'Doniv','Dubai'])
execute_command("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [3,'Keviv','Chennai'])
execute_command("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [4,'Ehtevs','Pune'])
execute_command("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [5,'Dnivog','Belgaum'])
execute_command("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [6,'Ateegk','Narewadi'])
execute_command("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [7,'KannabbuS','Yamkanmardi'])
execute_command("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [8,'Jonas','Atlanta'])
#</insertData>

#<GetNumberOfTables>
print("\nTable count per keyspace")
tableCount = session.execute("SELECT keyspace_name, table_name FROM system_schema.tables")
getTableCount(tableCount._current_rows)
#</GetNumberOfTables>

#<queryAllItems>
print ("\nSelecting All")
rows = session.execute('SELECT * FROM uprofile.user')
PrintTable(rows)
#</queryAllItems>

#<queryByID>
print ("\nSelecting Id=1")
rows = session.execute('SELECT * FROM uprofile.user where user_id=1')
PrintTable(rows)
#</queryByID>

cluster.shutdown()
