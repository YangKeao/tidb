"""Finite, explicit SQL experiment matrix; no production code or fake evaluator."""
from cases import DATABASE,TABLE,PERFORMANCE
TABLE_SQL=f'{DATABASE}.{TABLE}'
CONFIGS={'batch1024':(1024,1),'batch128':(128,1),'batch32':(32,1),'vecOff':(1024,0)}
FAMILIES=PERFORMANCE+[
 ('float','id, SIN(x)+COS(x)'),
 ('temporal','id, YEAR(ts), MONTH(ts), DAYOFMONTH(ts), MICROSECOND(ts)'),
 ('hash','id, SHA2(s,256)'),
 ('json_modify',"id, JSON_SET(JSON_OBJECT('k',id%7),'$.k',j)"),
 ('unicode_regexp',"id, REGEXP_REPLACE(wide,'[A-Z]','x')"),
 ('short_circuit',"id, IF(dense<0,JSON_UNQUOTE(JSON_EXTRACT(JSON_OBJECT('k',j),'$.k')),NULL)"),
]
def projection_case(group,label,projection,count=4096,config='batch1024',**extra):
 where='id=7' if count==1 else f'id<{count}'
 return {'group':group,'case':label,'config':config,'rows':count,'sql':f'SELECT {projection} FROM {TABLE_SQL} WHERE {where} ORDER BY id',**extra}
def matrix():
 result=[]
 for config in ['batch1024','batch32','vecOff']:
  result.extend(projection_case('family',label,p,config=config) for label,p in FAMILIES)
 for label,p in FAMILIES:
  if label not in ['scan','integer','nested_integer','decimal','text','json']:continue
  result.extend(projection_case('rows',label,p,count=n) for n in [1,16,256,1024,8192])
 for label,p in FAMILIES:
  if label in ['integer','nested_integer','json','wide_text']:
   result.append(projection_case('chunk',label,p,config='batch128'))
 for depth in [1,4,8,16,32]:
  p='dense'
  for _ in range(depth):p=f'({p}+j)'
  result.append(projection_case('depth',f'depth{depth}','id, '+p,depth=depth))
 for width in [1,4,8,16]:
  result.append(projection_case('width',f'columns{width}','id, '+','.join(f'dense+j+{i}' for i in range(width)),width=width))
 for size in [8,64,256,1024]:
  result.append(projection_case('string_length',f'chars{size}',f'id, LOWER(REPEAT(s,{size}))',count=256,input_repeat=size))
 for probability,predicate in [('never','id<0'),('half','dense<0'),('always','id>=0')]:
  result.append(projection_case('demand',probability,f"id, IF({predicate},JSON_UNQUOTE(JSON_EXTRACT(JSON_OBJECT('k',id%7),'$.k')),NULL)"))
 for density,column in [('0','dense'),('10','nullable'),('99','sparse'),('100','allnull')]:
  result.append(projection_case('null_density',density,f'id, {column}+j'))
 result.extend([
  {'group':'executor','case':'filter','config':'batch1024','rows':None,'sql':f'SELECT id FROM {TABLE_SQL} WHERE id<4096 AND nullable+j>30 ORDER BY id'},
  {'group':'executor','case':'topn','config':'batch1024','rows':20,'sql':f'SELECT id,nullable FROM {TABLE_SQL} WHERE id<4096 ORDER BY nullable DESC,id ASC LIMIT 20'},
  {'group':'executor','case':'aggregate','config':'batch1024','rows':1,'sql':f'SELECT COUNT(nullable),SUM(nullable),MIN(dense),MAX(j) FROM {TABLE_SQL} WHERE id<4096'},
  {'group':'executor','case':'join','config':'batch1024','rows':128,'sql':f'SELECT a.id,b.j FROM {TABLE_SQL} a JOIN {TABLE_SQL} b ON b.id=a.id+1 WHERE a.id<128 ORDER BY a.id'},
  {'group':'warning','case':'cast_invalid','config':'batch1024','rows':256,'sql':f"SELECT id,CAST(CONCAT(s,'x') AS SIGNED) FROM {TABLE_SQL} WHERE id<256 ORDER BY id"},
 ])
 assert len({key(c) for c in result})==len(result)
 return result
def key(case):return f"{case['group']}/{case['case']}/{case['config']}/{case['rows']}"
def subset(labels):
 return [projection_case('concurrency',label,p) for label,p in FAMILIES if label in labels]
if __name__=='__main__':
 import json,collections
 cases=matrix();assert len(cases)==113,len(cases)
 assert len({c['sql'] for c in cases})==69
 print(json.dumps(dict(collections.Counter(c['group'] for c in cases)),indent=2))
