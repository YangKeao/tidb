import json,time,hashlib
import pymysql
from cluster import *
from cases import DATABASE,TABLE,ROWS
# The bounded node cannot load stored JSON columns in either revision. Preserve
# that actual refusal and original table; derive a second real typed table.
free_ports(['go','go_status'])
p=launch('go',['--store','tikv','--path',f'127.0.0.1:{PORTS["pd"]}','--host','127.0.0.1','-P',str(PORTS['go']),'--status',str(PORTS['go_status']),'--lease','1s','--log-file',str(RUNTIME/'go.log')]);wait_port('go',p,300)
conn=None
for _ in range(180):
 try:
  conn=pymysql.connect(host='127.0.0.1',port=PORTS['go'],user='root',password='',autocommit=True,charset='utf8mb4');break
 except pymysql.MySQLError:time.sleep(1)
assert conn
with conn.cursor() as c:
 c.execute(f'CREATE TABLE {DATABASE}.{TABLE} LIKE {DATABASE}.t')
 c.execute(f'ALTER TABLE {DATABASE}.{TABLE} DROP COLUMN doc')
 c.execute(f'INSERT INTO {DATABASE}.{TABLE} SELECT id,dense,nullable,sparse,allnull,j,s,d,x,ts,wide FROM {DATABASE}.t')
 c.execute(f'SELECT COUNT(*),SUM(dense),COUNT(nullable),COUNT(sparse) FROM {DATABASE}.{TABLE}');counts=c.fetchone();assert counts[0]==ROWS
(RUNTIME/'fixture-derived.json').write_text(json.dumps({'database':DATABASE,'table':TABLE,'source_table':'t','rows':ROWS,'counts':[str(v) for v in counts],'reason':'Both native and current bounded Rust nodes refuse catalog containing JSON; JSON evaluation probes use genuine computed JSON from id instead.','cases_sha256':hashlib.sha256((BASE/'cases.py').read_bytes()).hexdigest()},indent=2)+'\n')
conn.close();stop('go');print('derived real table ready; original stored-JSON fixture/refusals retained; Go stopped')
