#!/usr/bin/env python2.7

import dpkt, pcap, datetime, socket, time, sys, os

def inet_to_str(inet):
    try: return socket.inet_ntop(socket.AF_INET, inet)
    except ValueError:
        return socket.inet_ntop(socket.AF_INET6, inet)

data = {}
pc = pcap.pcap(sys.argv[1])
last_cp = time.time()
dst_file, tmp_file = sys.argv[2], sys.argv[2] + '.tmp'
write_timeout = int(sys.argv[3])
my_ips = set(sys.argv[4].split(' '))
my_nets = sys.argv[5].split(' ')
for _, pkt in pc:
    eth = dpkt.ethernet.Ethernet(pkt)
    the_ip, out_bytes, in_bytes = None, 0, 0
    try:
        ip = eth.data
        src_ip, dst_ip = inet_to_str(ip.src), inet_to_str(ip.dst)
        try: dat_bytes = ip.len
        except: dat_bytes = ip.plen
    except Exception as err:
        print("error:", err)
        continue
    for net in my_nets:
        if src_ip.startswith(net) and src_ip not in my_ips:
            the_ip, out_bytes, in_bytes = src_ip, dat_bytes, 0
        elif dst_ip.startswith(net) and dst_ip not in my_ips:
            the_ip, out_bytes, in_bytes = dst_ip, 0, dat_bytes
    if the_ip is None: continue
    cur_out_bytes, cur_in_bytes, cur_pkts = data.get(the_ip, (0, 0, 0))
    data[the_ip] = (cur_out_bytes + out_bytes, cur_in_bytes + in_bytes, cur_pkts + 1)
    if time.time() - last_cp > write_timeout:
        last_cp = time.time()
        with open(tmp_file, 'w') as fd:
            for dkey, dval in data.items():
                fd.write("%s %d %d %d\n" % (dkey, dval[0], dval[1], dval[2]))
        os.rename(tmp_file, dst_file)
