from cluster import *
import subprocess,sys
start_storage();bootstrap();subprocess.run([sys.executable,str(BASE/'derive-fixture.py')],check=True)
stop('tikv');stop('pd');start_storage()
print('new fixture and post-DDL restart complete; no timing',flush=True)
