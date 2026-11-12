from pathlib import Path
from decimal import Decimal
import json,hashlib,struct,datetime,traceback
import pymysql
from cases import queries,DATABASE,TABLE
from cluster import BASE,RUNTIME,PORTS,records,birth,start_frontend,stop

def connect(name):
 credentials={'user':'root','password':''}
 return pymysql.connect(host='127.0.0.1',port=PORTS[name],user=credentials['user'],password=credentials['password'],autocommit=True,charset='utf8mb4',read_timeout=120,write_timeout=30)

def encode(v):
 if isinstance(v,Decimal):return {'decimal':str(v)}
 if isinstance(v,float):return {'f64bits':struct.pack('!d',v).hex()}
 if isinstance(v,bytes):return {'bytes':v.hex()}
 if isinstance(v,(datetime.datetime,datetime.date,datetime.time)):return {'time':v.isoformat()}
 if isinstance(v,datetime.timedelta):return {'timedelta':{'days':v.days,'seconds':v.seconds,'microseconds':v.microseconds}}
 if isinstance(v,(list,tuple)):return [encode(x) for x in v]
 return v

def metadata(c):
 return [{key:getattr(f,key) for key in ['name','type_code','charsetnr','flags','length','scale']} for f in c._result.fields]
