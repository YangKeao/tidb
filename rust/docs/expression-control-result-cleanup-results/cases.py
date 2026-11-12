"""Deterministic genuine SQL fixture and independent correctness checks."""
from decimal import Decimal, ROUND_HALF_UP
import json
DATABASE='expr_policy_optimization'
ROWS=8192
TABLE='t_eval'

def row(i):
    return (i, i%97-48, None if i%10==0 else i%97-48,
            i%97-48 if i%100==0 else None, None, i%13+1,
            None if i%10==0 else ['Alpha','béTa','abcdef','TiDB','vectorized','z'][i%6],
            None if i%17==0 else Decimal(i%10001-5000)/Decimal(10000),
            None if i%19==0 else (i%97-48)/8.0,
            None if i%23==0 else json.dumps({'k':i%7,'a':[i%3,i%5]},separators=(',',':')),
            None if i%29==0 else f'2020-01-{1+i%28:02d} 12:34:{i%60:02d}.123456',
            None if i%10==0 else 'AbCdéXYZ'*16)

SCHEMA=f'''CREATE DATABASE {DATABASE};
CREATE TABLE {DATABASE}.t (
 id BIGINT PRIMARY KEY, dense BIGINT NOT NULL, nullable BIGINT,
 sparse BIGINT, allnull BIGINT, j BIGINT NOT NULL,
 s VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
 d DECIMAL(18,4), x DOUBLE, doc JSON, ts DATETIME(6),
 wide VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
)'''

# Each oracle sees the original stored row, never another expression evaluator.
FUNCTIONAL=[
 ('integer', 'id, dense+j',lambda r:(r[0],r[1]+r[5])),
 ('nullable_integer','id, nullable+j',lambda r:(r[0],None if r[2] is None else r[2]+r[5])),
 ('sparse_integer','id, sparse+j',lambda r:(r[0],None if r[3] is None else r[3]+r[5])),
 ('all_null','id, allnull+j',lambda r:(r[0],None)),
 ('nested_integer','id, (nullable+j)+(dense-j)',lambda r:(r[0],None if r[2] is None else r[2]+r[1])),
 ('decimal_round','id, ROUND(d,2)',lambda r:(r[0],None if r[7] is None else r[7].quantize(Decimal('.01'),rounding=ROUND_HALF_UP))),
 ('lower','id, LOWER(s)',lambda r:(r[0],None if r[6] is None else r[6].lower())),
 ('substring','id, SUBSTRING(s,2,3)',lambda r:(r[0],None if r[6] is None else r[6][1:4])),
 ('control','id, IF(nullable IS NULL, dense, nullable+j)',lambda r:(r[0],r[1] if r[2] is None else r[2]+r[5])),
 ('json_extract',"id, JSON_UNQUOTE(JSON_EXTRACT(IF(id%23=0,NULL,JSON_OBJECT('k',id%7)),'$.k'))",lambda r:(r[0],None if r[9] is None else str(r[0]%7))),
 ('date_parts','id, YEAR(ts), MONTH(ts), DAYOFMONTH(ts), MICROSECOND(ts)',lambda r:(r[0],None if r[10] is None else 2020,None if r[10] is None else 1,None if r[10] is None else 1+r[0]%28,None if r[10] is None else 123456)),
]

def queries(limit=4096):
    return [(label,f'SELECT {projection} FROM {DATABASE}.{TABLE} WHERE id < {limit} ORDER BY id', [oracle(row(i)) for i in range(limit)]) for label,projection,oracle in FUNCTIONAL]

PERFORMANCE=[
 ('scan','id, dense, nullable'),
 ('integer','id, dense+j'),
 ('nested_integer','id, ((nullable+j)+(dense-j))+(dense+j)'),
 ('null10','id, nullable+j'),
 ('null99','id, sparse+j'),
 ('null100','id, allnull+j'),
 ('decimal','id, ROUND(d+j,2)'),
 ('text','id, SUBSTRING(LOWER(s),2,3)'),
 ('wide_text','id, LOWER(wide)'),
 ('control','id, IF(nullable IS NULL,dense,nullable+j)'),
 ('json',"id, JSON_UNQUOTE(JSON_EXTRACT(IF(id%23=0,NULL,JSON_OBJECT('k',id%7)),'$.k'))"),
 ('regexp',"id, REGEXP_LIKE(s,'^[a-zA-Z]+$')"),
]
