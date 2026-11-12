import json
from cluster import BASE,stop
from common import guard,start,configure
from functional import connect,encode,metadata
guard()
sql="SELECT `JSON_SUM_CRC32`(IF(id%23=0,NULL,JSON_ARRAY(id%3,id%5))) FROM expr_policy_optimization.t_eval WHERE id<8 ORDER BY id"
results=[]
try:
 for mode in ['native','current','frozen']:
  start(mode)
  try:
   with connect(mode) as conn:
    with conn.cursor() as c:
     configure(c,'batch1024');item={'mode':mode,'sql':sql}
     try:c.execute(sql);item['rows']=encode(c.fetchall());item['fields']=metadata(c)
     except Exception as error:item['error']=str(error)
     results.append(item)
  finally:stop(mode)
finally:
 (BASE/'json-entry-probe2.json').write_text(json.dumps({'timing':False,'reason':'Derived fixture omits stored JSON column; use computed array and existing quoted internal function name','results':results},indent=2)+'\n')
print(json.dumps(results))
