#!/usr/bin/env python3.8

import sys, time, os, urllib.request, socket, random

URL = "http://%s/accounting/ip.cgi" % sys.argv[1]
directory = sys.argv[2]
curtime = time.localtime()
data = {}
dns_cache = {}

def gen_id() -> int:
    return int(time.time()*10000000000000) * 1000 + random.randint(0, 999)
def id_str(id: int) -> str:
    return id.to_bytes(11, 'big').hex()

for row in urllib.request.urlopen(URL):
    src_ip, dst_ip, dat_bytes, dat_pkgs, _, _ = row.decode('utf-8').strip().split(' ')
    dat_pkgs, dat_bytes = int(dat_pkgs), int(dat_bytes)
    if src_ip not in dns_cache:
        src_name = src_ip
        if src_ip.startswith('10.10.1.') or src_ip.startswith('2a01:4f8:191:41a6:'):
            try: src_name = socket.gethostbyaddr(src_ip)[0]
            except: pass
        dns_cache[src_ip] = src_name
    else: src_name = dns_cache[src_ip]
    if dst_ip not in dns_cache:
        dst_name = dst_ip
        if dst_ip.startswith('10.10.1.') or dst_ip.startswith('2a01:4f8:191:41a6:'):
            try: dst_name = socket.gethostbyaddr(dst_ip)[0]
            except: pass
        dns_cache[dst_ip] = dst_name
    else: dst_name = dns_cache[dst_ip]
    if src_name.endswith('.kozachuk.info') and src_ip not in ('10.10.1.8', '2a01:4f8:191:41a6:10:1:0:8'):
        the_name = src_name.replace('.kozachuk.info', '')
        out = True
    elif dst_name.endswith('.kozachuk.info') and dst_ip not in ('10.10.1.8', '2a01:4f8:191:41a6:10:1:0:8'):
        the_name = dst_name.replace('.kozachuk.info', '')
        out = False
    else: continue
    if the_name not in data:
        data[the_name] = {'in': 0, 'out': 0, 'pkg': 0, 'id': gen_id()}
    if out: data[the_name]['out'] += dat_bytes
    else: data[the_name]['in'] += dat_bytes
    data[the_name]['pkg'] += dat_pkgs
    
for hostname, host in data.items():
    try: os.mkdir(directory + hostname)
    except: pass
    if host['in'] == 0 and host['out'] == 0 and host['pkg'] == 0:
        continue
    fname = "%s%s/day_%02d%02d%02d" % (directory, hostname, curtime.tm_year, curtime.tm_mon, curtime.tm_mday)
    write_data = '%d %d %d %d %d mikrotik %s\n' \
        % (curtime.tm_hour, curtime.tm_min, host['in'], host['out'], host['pkg'], id_str(host['id']))
    with open(fname + '.tmp', 'a') as fd:
        try:
            with open(fname) as fdo:
                fd.write(fdo.read())
        except FileNotFoundError: pass
        fd.write(write_data)
    os.rename(fname + '.tmp', fname)
