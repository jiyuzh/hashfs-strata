import psutil

def resolve_unit(s):
    ''' Converts human readable representations of bytes to their fully expanded
        form. Ex: 1k = 1024. Works for 'k', 'm', and 'g'. '''
    c = s[-1].lower()
    unit = 1
    if c == 'k':
        unit = 1024
    elif c == 'm':
        unit = 1024 ** 2
    elif c == 'g':
        unit = 1024 ** 3
    
    if c.isnumeric():
        return int(s)

    return int(s[:-1]) * unit

def resolve_units(l):
    ''' Resolves an array of unit strings. '''
    return [resolve_unit(x) for x in l]

def do_nothing(*_, **__):
    ''' Literally does nothing. '''

def find_pid_in_file(pid, pid_file):
    ''' 
        Checks if the given PID (or any of the process's children PIDs) is in a
        given PID file (a file which only contains a single PID). 
    '''

    with pid_file.open() as f:
        pid_from_file = int(f.read())
        child_pids = [x.pid for x in psutil.Process(pid).children(recursive=True)]
        if pid_from_file in child_pids:
            return True
        return False

# For benchmarking stability:
# https://easyperf.net/blog/2019/08/02/Perf-measurement-environment-on-Linux#1-disable-turboboost

def disable_turboboost():
    ''' Tip 1: Disabling turboboost. '''
    cmd = 'echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo'

def disable_hyperthreading():
    ''' Tip 2: Disable hypterthreading. '''
    cmd = '/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list'
    # lists "real,ht"
    cmd = 'echo 0 > /sys/devices/system/cpu/cpuX/online'

def set_scaling_governer():
    ''' Tip 3 '''
    cmd = 'echo performance > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_governor'

def set_cpu_affinity(numa_node):
    pass

def set_process_priority():
    pass

def disable_ASLR():
    cmd = 'echo 0 | sudo tee /proc/sys/kernel/randomize_va_space'
