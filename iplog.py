#!/usr/bin/env python3.8

import sys, os, socket, time, random
from typing import Dict

directory = sys.argv[4]
dns_cache = {}
host_data: Dict[str, Dict[str, int]] = {}
last_data = {}
curtime = time.localtime()
data_file = sys.argv[2]
lastusage = sys.argv[1]
router = sys.argv[3]

def gen_id() -> int:
    return int(time.time()*100000000000000) * 1000 + random.randint(0, 999)
def id_str(id: int) -> str:
    return id.to_bytes(11, 'big').hex()

with open(lastusage) as fd:
    for line in fd:
        hostname, host_in_str, host_out_str, host_pkg_str = line.strip().split(' ')
        host_in, host_out, host_pkg = int(host_in_str), int(host_out_str), int(host_pkg_str)
        last_data[hostname] = {'in': host_in, 'out': host_out, 'pkg': host_pkg}

with open(data_file) as fd:
    for line in fd:
        try:
            the_ip, dat_out_str, dat_in_str, dat_pkg_str = line.strip().split(' ')
            if the_ip not in dns_cache:
                try: the_name = socket.gethostbyaddr(the_ip)[0]
                except: the_name = the_ip
                dns_cache[the_ip] = the_name
            else: the_name = dns_cache[the_ip]
            dat_out, dat_in, dat_pkg = int(dat_out_str), int(dat_in_str), int(dat_pkg_str)
        except: continue
        host = host_data.setdefault(the_name.replace('.kozachuk.info', ''), {})
        host['out'] = host.get('out', 0) + dat_out
        host['in']  = host.get('in', 0)  + dat_in
        host['pkg'] = host.get('pkg', 0) + dat_pkg
        host['id'] = gen_id()

with open(lastusage + '.tmp', 'w') as fd:
    new_data: Dict[str, Dict[str, int]] = {}
    for hostname, host in host_data.items():
        new_data[hostname] = {}
        if hostname in last_data:
            for k in host.keys():
                newv = host[k] - last_data[hostname][k]
                if newv < 0: newv = 0
                new_data[hostname][k] = newv
        else:
            for k in host.keys():
                new_data[hostname][k] = host[k]
        fd.write('%s %d %d %d\n' % (hostname, host['in'], host['out'], host['pkg']))
os.rename(lastusage + '.tmp', lastusage)

for hostname, host in new_data.items():
    try: os.mkdir(directory + hostname)
    except: pass
    if host['in'] == 0 and host['out'] == 0 and host['pkg'] == 0:
        continue
    fname = "%s%s/day_%02d%02d%02d" \
        % (directory, hostname, curtime.tm_year, curtime.tm_mon, curtime.tm_mday)
    with open(fname + '.tmp', 'a') as fd:
        fd.write('%d %d %d %d %d %s %s\n' \
                 % (curtime.tm_hour, curtime.tm_min,
                    host['in'], host['out'], host['pkg'],
                    router, id_str(host['id'])))
    os.rename(fname + '.tmp', fname)
