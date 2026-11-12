"""Losslessly deduplicate repeated semantic observations for Git review."""
import hashlib,json

def digest(data):
    return hashlib.sha256(data).hexdigest()

def encode(raw):
    pool={};records=[]
    def add(value):
        key=digest(json.dumps(value,separators=(',',':')).encode())
        if key in pool:
            assert pool[key]==value
        else:
            pool[key]=value
        return key
    for line in raw.decode().splitlines():
        record=json.loads(line)
        if 'observations' in record:
            record['observations']=[add(value) for value in record['observations']]
        if 'observation' in record:
            record['observation']=add(record['observation'])
        records.append(record)
    result={'format':'observation dictionary v1','original_sha256':digest(raw),'pool':pool,'records':records}
    assert decode(result)==raw
    return result

def decode(value):
    assert value['format']=='observation dictionary v1'
    pool=value['pool'];result=[]
    for key,observation in pool.items():
        assert key==digest(json.dumps(observation,separators=(',',':')).encode())
    for item in value['records']:
        record=dict(item)
        if 'observations' in record:
            record['observations']=[pool[key] for key in record['observations']]
        if 'observation' in record:
            record['observation']=pool[record['observation']]
        result.append(json.dumps(record)+'\n')
    raw=''.join(result).encode()
    assert digest(raw)==value['original_sha256']
    return raw

if __name__=='__main__':
    raw=b'{"phase": "before", "observations": [{"rows": 1}, {"rows": 1}]}\n{"phase": "after", "observation": {"rows": 1}}\n'
    value=encode(raw)
    assert len(value['pool'])==1 and decode(value)==raw
    value['records'][0]['phase']='tampered'
    try:
        decode(value)
    except AssertionError:
        pass
    else:
        raise AssertionError('tampered observation record accepted')
    print('lossless observation storage selfcheck PASS')
