"""Linux POSIX process CPU clock, avoiding /proc stat tick quantization."""
import ctypes,os,time,json
_lib=ctypes.CDLL(None,use_errno=True)
_getid=_lib.clock_getcpuclockid
_getid.argtypes=[ctypes.c_int,ctypes.POINTER(ctypes.c_int)]
_getid.restype=ctypes.c_int
_cache={}
def clock_id(pid):
 if pid not in _cache:
  out=ctypes.c_int();error=_getid(pid,ctypes.byref(out))
  if error:raise OSError(error,os.strerror(error))
  _cache[pid]=out.value
 return _cache[pid]
def cpu_ns(pid):return time.clock_gettime_ns(clock_id(pid))
def resolution_ns(pid):return round(time.clock_getres(clock_id(pid))*1e9)
def selfcheck():
 pid=os.getpid();a=cpu_ns(pid);b=time.process_time_ns();assert abs(a-b)<1000000
 wall=time.monotonic_ns();before=cpu_ns(pid);time.sleep(.04);sleep_cpu=cpu_ns(pid)-before;assert time.monotonic_ns()-wall>=30000000 and sleep_cpu<20000000
 before=cpu_ns(pid);start=time.process_time_ns();value=1
 while time.process_time_ns()-start<60000000:value=(value*1664525+1013904223)&0xffffffff
 busy=cpu_ns(pid)-before;assert 50000000<=busy<120000000
 assert 0<resolution_ns(pid)<=1000
 return {'clock':'clock_getcpuclockid(pid), process CPU; time.clock_gettime_ns','header_prototype':'/usr/include/time.h:347','resolution_ns':resolution_ns(pid),'proc_stat_tick_ns':1_000_000_000//os.sysconf('SC_CLK_TCK'),'self_process_time_agreement_better_than_ns':1000000,'sleep_cpu_ns':sleep_cpu,'busy_cpu_ns':busy,'scope':'clock API/self-process semantics only; not a production behavior or performance benchmark'}
if __name__=='__main__':print(json.dumps(selfcheck(),indent=2))
