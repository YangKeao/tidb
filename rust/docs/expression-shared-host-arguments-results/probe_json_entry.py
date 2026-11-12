from pathlib import Path
import json,time
from cluster import BASE,records,birth,RUNTIME,start_storage,stop
from common import guard,start,configure
from functional import connect,encode,metadata
assert not (BASE/'primary.jsonl').exists()
assert json.loads((BASE/'measurement1.receipt.json').read_text())['returncode']==1
guard()
for name,r in records().items():assert birth(r['pid'])!=r['birth'],name
for owner,name in [('pd','pd-data'),('tikv','tikv-data')]:
 r=records()[owner];path=RUNTIME/name
 assert r['command'][r['command'].index('--data-dir')+1]==str(path) and path.is_dir() and not path.is_symlink()
start_storage()
queries=["SELECT `JSON_SUM_CRC32`(JSON_EXTRACT(doc,'$.a')) FROM expr_policy_optimization.t_eval WHERE id<8 ORDER BY id", "SELECT JSON_SUM_CRC32(JSON_EXTRACT(doc,'$.a') AS UNSIGNED ARRAY) FROM expr_policy_optimization.t_eval WHERE id<8 ORDER BY id"]
results=[]
try:
 for mode in ['native','current','frozen']:
  start(mode)
  try:
   with connect(mode) as conn:
    with conn.cursor() as c:
     configure(c,'batch1024')
     for sql in queries:
      item={'mode':mode,'sql':sql}
      try:c.execute(sql);item['rows']=encode(c.fetchall());item['fields']=metadata(c)
      except Exception as error:item['error']=str(error)
      results.append(item)
  finally:stop(mode)
finally:
 (BASE/'json-entry-probe.json').write_text(json.dumps({'timing':False,'reason':'All versions rejected initial plain function syntax; inspect existing supported entry without product edits','results':results},indent=2)+'\n')
print(json.dumps(results))
