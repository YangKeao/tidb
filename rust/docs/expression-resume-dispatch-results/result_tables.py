"""Render every fixed scene and control, only from audited completed cells."""
from pathlib import Path
import json
P=Path(__file__).resolve().parent
NAMES={'family/scan/batch1024/4096':'扫描4096行','family/integer/batch1024/4096':'整数4096行','family/decimal/batch1024/4096':'Decimal4096行','family/text/batch1024/4096':'文本4096行','shape/deep16/batch1024/1':'深16层／1行','shape/deep16/batch1024/4096':'深16层／4096行',**{'arguments/'+k+'/batch1024/4096':v for k,v in [('crypto','MD5／SHA2／PASSWORD'),('compression','压缩／解压'),('uuid','UUID转换'),('calendar','日期／格式／Unix时间'),('timestampdiff','时间差／MAKETIME'),('extract_parse','EXTRACT／STR_TO_DATE'),('date_arithmetic','DATE_ADD／DATE_SUB'),('json_value','JSON构造／提取')]}}

METRICS=[('cpu_ms_per_query','CPU ms/查询'),('p50_ms','p50 ms'),('p95_ms','p95 ms'),('qps','QPS')]
def pct(v):return f'{(v-1)*100:+.3f}%'
def ran(v):return ' … '.join(pct(x) for x in v)
def table(headers,rs):return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |',*['| '+' | '.join(str(v) for v in r)+' |' for r in rs]])
def render(s):
 rows=s['primary'];assert len(rows)==14 and {r['key'] for r in rows}==set(NAMES)
 tables=[]
 for metric,label in METRICS:
  tables.append(table(['负载',*[m+' '+label for m in ['A','B','C','F']],'C/A变化','C/B变化'],[[NAMES[r['key']],*[f'{r[m][metric]:.6f}' for m in ['native','retained_b','current','frozen']],pct(r['after_over_native'][metric]),pct(r['after_over_retained_b'][metric])] for r in rows]))
  for den,l in [('native','C/A'),('retained_b','C/B'),('frozen','C/F')]:
   tables.append(table(['负载',l+' '+label+'成对中位变化','四轮观察范围'],[[NAMES[r['key']],pct(r['after_over_'+den][metric]),ran(r['after_over_'+den+'_range'][metric])] for r in rows]))
  tables.append(table(['负载','同版B/A '+label+'变化','四轮观察范围'],[[NAMES[r['key']],pct(r['retained_b_over_retained_a'][metric]),ran(r['retained_b_over_retained_a_range'][metric])] for r in rows]))
 return '\n\n'.join(tables)
def directional(s):
 out=[]
 for r in s['primary']:
  for metric,_ in METRICS:
   for den in ['native','retained_b','frozen']:
    lo,hi=r['after_over_'+den+'_range'][metric]
    out.append({'key':r['key'],'metric':metric,'denominator':den,'paired_median':r['after_over_'+den][metric],'observed_range':[lo,hi],'direction':'lower_all_four' if hi<1 else 'higher_all_four' if lo>1 else 'mixed_or_tie','favorability':'higher_good' if metric=='qps' else 'lower_good'})
 return out
if __name__=='__main__':
 assert json.loads((P/'audit-final.json').read_text())['status']=='PASS'
 s=json.loads((P/'summary.json').read_text());(P/'all-result-tables.md').write_text(render(s)+'\n');(P/'all-directional-results.json').write_text(json.dumps(directional(s),indent=2)+'\n');print('all14 scenes,4metrics and same-version controls rendered; no retention inferred')
