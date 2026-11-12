"""Balanced paired SQL comparison of preserved before and final after binaries."""
import json,random,time
from cluster import BASE,stop
from common import prepare_environment,start,configure,read_configuration,observe,clock_cell,binary,write
from functional import connect,encode
from workloads import FAMILIES,projection_case,key

def all_cases():
 chosen={'scan','integer','nested_integer','decimal','text','json','control','float'}
 result=[projection_case('family',label,p) for label,p in FAMILIES if label in chosen]
 result += [projection_case('rows',label,p,count=1) for label,p in FAMILIES if label in {'integer','json','text'}]
 p='dense'
 for _ in range(16):p=f'({p}+j)'
 result += [projection_case('shape','deep16','id, '+p),projection_case('shape','width8','id, '+','.join(f'dense+j+{i}' for i in range(8)))]
 result += [projection_case('configuration','json',dict(FAMILIES)['json'],config=c) for c in ['batch32','vecOff']]
 result += [projection_case('read_sites',label,sql) for label,sql in [('cast_char','id, CAST(nullable AS CHAR)'),('in','id, nullable IN (dense,j,NULL)'),('interval','id, INTERVAL(nullable,-20,0,20)'),('greatest','id, GREATEST(nullable,dense,j)')]]
 result += [projection_case('row_sites',label,sql) for label,sql in [('lower_upper','id, LOWER(s), UPPER(s)'),('concat','id, CONCAT(s,wide)'),('strcmp','id, STRCMP(s,wide)'),('regexp_like',"id, REGEXP_LIKE(s,'^[a-z]')")]]
 assert len(result)==23 and len({key(c) for c in result})==23
 return result
MODES=['native','current','retained_b','frozen']
LABELS={'native':'retained A actual macro14 2d435','current':'shared host arguments task29 candidate','retained_b':'retained B SAME actual macro14 ELF as A','frozen':'original native actual99a'}
ORDERS=[['native','current','frozen','retained_b'],['current','retained_b','native','frozen'],['retained_b','frozen','current','native'],['frozen','native','retained_b','current']]
def cases():
 wanted={'family/scan/batch1024/4096','family/decimal/batch1024/4096','family/integer/batch1024/4096','family/text/batch1024/4096','shape/deep16/batch1024/4096'}
 result=[c for c in all_cases() if key(c) in wanted]
 p='dense'
 for _ in range(16):p=f'({p}+j)'
 result.append(projection_case('shape','deep16','id, '+p,count=1))
 result += [projection_case('arguments',name,'id, '+expr) for name,expr in [
  ('crypto',"MD5(s), SHA2(s,256), PASSWORD(s)"),
  ('compression',"UNCOMPRESS(COMPRESS(s)), UNCOMPRESSED_LENGTH(COMPRESS(s))"),
  ('uuid',"UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db',id%2), IS_UUID(s)"),
  ('calendar',"YEAR(ts), MONTH(ts), DATE_FORMAT(ts,'%Y-%m-%d'), UNIX_TIMESTAMP(ts)"),
  ('timestampdiff',"TIMESTAMPDIFF(SECOND,ts,DATE_ADD(ts,INTERVAL j SECOND)), MAKETIME(id%24,j%60,1)"),
  ('extract_parse',"EXTRACT(DAY FROM ts), STR_TO_DATE(DATE_FORMAT(ts,'%Y-%m-%d'),'%Y-%m-%d')"),
  ('date_arithmetic',"DATE_ADD(ts,INTERVAL j DAY), DATE_SUB(ts,INTERVAL j SECOND)"),
  ('json_value',"JSON_UNQUOTE(JSON_EXTRACT(IF(id%23=0,NULL,JSON_OBJECT('k',id%7)),'$.k'))")]]
 assert len(result)==14 and len({key(c) for c in result})==14
 return result
assert len(cases())==14
for m in MODES:assert sorted(order.index(m) for order in ORDERS)==list(range(4))
for i,a in enumerate(MODES):
 for b in MODES[i+1:]:assert sum(order.index(a)<order.index(b) for order in ORDERS)==2
import collections
assert set(collections.Counter((a,b) for order in ORDERS for a,b in zip(order,order[1:])).values())=={1}

def preflight():
 out=BASE/'preflight.json';assert not out.exists();prepare_environment();observations=[]
 for mode in MODES:
  pid,_=start(mode)
  try:
   with connect(mode) as conn:
    with conn.cursor() as c:
     for case in cases():
      item={'mode':mode,'key':key(case),'case_spec':case}
      try:
       item['session_configuration']=configure(c,case['config']);item.update(observe(c,case));c.execute('EXPLAIN '+case['sql']);item['explain']=encode(c.fetchall())
      except Exception as e:item['error']=str(e)
      observations.append(item);write('preflight-progress.json',{'completed':len(observations),'total':56,'last_key':key(case),'mode':mode})
  finally:stop(mode)
 indexed={(i['mode'],i['key']):i for i in observations};pairs=[];failures=[]
 for case in cases():
  k=key(case);ref=indexed['native',k];checks={}
  for mode in MODES[1:]:
   other=indexed[mode,k];checks[mode]='error' not in ref and 'error' not in other and all(ref[f]==other[f] for f in ['rows','digest','fields','warnings','explain','session_configuration'])
   if not checks[mode]:failures.append({'key':k,'mode':mode,'reason':'result, metadata, warning, exact full plan or effective configuration differs','reference_error':ref.get('error'),'other_error':other.get('error')})
  pairs.append({'key':k,'case_spec':case,'equivalent':all(checks.values()),'comparisons_to_A':checks})
 write('preflight.json',{'mode_labels':LABELS,'pairs':pairs,'observations':observations,'failures':failures,'timing':False});assert not failures,failures
 print('Strict56 observations PASS',flush=True)

def measure():
 out=BASE/'primary.jsonl';assert not out.exists();prepare_environment();pre=json.loads((BASE/'preflight.json').read_text());assert not pre['failures'];pairs=pre['pairs'];refs={(o['mode'],o['key']):o for o in pre['observations']}
 write('primary-manifest.json',{'mode_labels':LABELS,'orders':ORDERS,'rounds':4,'case_count':len(pairs),'minimum_queries':30,'minimum_seconds':4,'maximum_queries':None,'scope':'whole SQL execute plus full fetch, target process CPU includes planning, RPC and encoding; client decoding affects elapsed time; not expression kernel timing','balanced_order_assertions':True,'effective_configuration_saved_each_cell':True})
 done=0
 with out.open('w',buffering=1) as f:
  for repeat,order in enumerate(ORDERS):
   cells=list(pairs);random.Random(77231+repeat).shuffle(cells)
   for mode in order:
    pid,startup=start(mode)
    try:
     with connect(mode) as conn:
      with conn.cursor() as c:
       for pair in cells:
        case=pair['case_spec'];k=key(case);actual_configuration=configure(c,case['config']);ref=refs[mode,k];assert actual_configuration==ref['session_configuration']
        warm_observations=[observe(c,case) for _ in range(2)]
        warm=warm_observations[-1]
        with (BASE/'cell-observations.jsonl').open('a') as obs:
         obs.write(json.dumps({'repeat':repeat,'mode':mode,'key':k,'phase':'before','configuration':actual_configuration,'observations':warm_observations})+'\n')
        assert all(all(w[x]==ref[x] for x in ['rows','digest','fields','warnings']) for w in warm_observations)
        def execute():
         c.execute(case['sql']);data=c.fetchall();assert len(data)==ref['rows']
        result=clock_cell(pid,execute,minimum_seconds=4.0,minimum_queries=30)
        final=observe(c,case)
        after_configuration=read_configuration(c,case['config'])
        with (BASE/'cell-observations.jsonl').open('a') as obs:
         obs.write(json.dumps({'repeat':repeat,'mode':mode,'key':k,'phase':'after','configuration':after_configuration,'observation':final})+'\n')
        assert all(final[x]==ref[x] for x in ['rows','digest','fields','warnings'])
        assert after_configuration==actual_configuration
        f.write(json.dumps({'repeat':repeat,'mode':mode,'mode_position':order.index(mode),'key':k,**case,**result,'session_configuration':actual_configuration,'session_configuration_after':after_configuration,'startup_seconds':startup,'binary_sha256':binary(mode)},separators=(',',':'))+'\n');done+=1
        write('primary-progress.json',{'completed':done,'total':224,'last_key':k,'mode':mode,'repeat':repeat})
    finally:stop(mode)
    print('Completed block',repeat,mode,flush=True)
 prepare_environment()
if __name__=='__main__':
 import sys
 if sys.argv[1]=='preflight':preflight()
 elif sys.argv[1]=='measure':measure()
 else:raise ValueError(sys.argv[1])
