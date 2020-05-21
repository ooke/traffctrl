#!/usr/bin/env python3.8

import sys, os, pprint, urllib.parse, hashlib
from datetime import datetime, timedelta
sys.path.insert(0, '/service/traffanalyze/client')

import dominate
from dominate.tags import *
from utils import *
from additionals import Additionals
from protocols import AddsEntry

inp_data = sys.stdin.read()
FNAME='/service/traffanalyze/data/additional_contingent.dat'
routerlist = ('nihonium',)
adds = Additionals(FNAME)
form_data = dict(urllib.parse.parse_qsl(inp_data, keep_blank_values = True))

doc = dominate.document(title='Additional contingent')
with doc.head:
    meta(charset='utf-8')
    meta(name="viewport", content="width=device-width, initial-scale=1, shrink-to-fit=no")
    link(rel='stylesheet', href='/css/jquery-ui.min.css')
    link(rel='stylesheet', href='/css/bootstrap.min.css')
    link(rel='stylesheet', href='/css/bootstrap-reboot.min.css')
    script(type='text/javascript', src='/js/jquery-3.5.1.min.js')
    script(type='text/javascript', src='/js/jquery-ui.js')
    script(type='text/javascript', src='/js/popper.js')
    script(type='text/javascript', src='/js/bootstrap.min.js')

data_list = []
for router in routerlist:
    for add_data in adds[router]:
        cksum = 'del_' + hashlib.md5(repr(add_data).encode('utf-8')).hexdigest()
        ignore = False
        for key_name in form_data.keys():
            if key_name == cksum:
                ignore = True
        if not ignore:
            data_list.append(add_data)
if 'add' in form_data:
    try:
        ts = datetime.strptime(form_data['ts'], '%Y-%m-%d %H:%M')
        atype = form_data['type']
        router = form_data['router'].strip()
        host = form_data['host'].strip()
        amount_str = form_data['amount'].strip()
        comment = form_data['comment'].strip()
        if len(comment) == 0: comment = None
        if atype == 'boost':
            host, amount = None, timedelta(hours = int(amount_str))
        elif atype == 'data': amount = units2bytes(amount_str)
        else: raise RuntimeError('Wront type.')
        data_list.append(AddsEntry(ts, atype, router, host, amount, comment))
    except Exception as err:
        sys.stderr.write('ERROR ADD: %s\n' % repr(err))
data_list.sort()
with open(FNAME + '.tmp', 'w') as fd:
    for add_data in data_list:
        fd.write("%s " % add_data.ts.strftime('%Y-%m-%d %H:%M'))
        fd.write("%s " % add_data.atype)
        fd.write("%s " % add_data.router)
        if add_data.host is not None:
            fd.write("%s " % add_data.host)
        if add_data.atype == 'boost':
            fd.write("%dh" % (add_data.amount.seconds / 3600))
        elif add_data.atype == 'data':
            fd.write(bytes2units(add_data.amount, delimiter = ''))
        if add_data.comment is not None:
            fd.write(" %s" % add_data.comment)
        fd.write("\n")
os.rename(FNAME + '.tmp', FNAME)
data_list.reverse()

with doc:
    a(href="../usage.html")
    with form(cls="form-horizontal", action="/cgi-bin/adds_formular.py", method="post"):
        with fieldset():
            legend(text="Additional contingent")

            with div(cls="container"):
                with div(cls="row clearfix"):
                    with div(cls="col-md-12 table-responsive"):
                        with table(cls="table table-bordered table-hover table-sortable", id="tab_logic"):
                            with thead():
                                with tr():
                                    th("timestamp", cls="text-center")
                                    th("type", cls="text-center")
                                    th("router", cls="text-center")
                                    th("host", cls="text-center")
                                    th("amount", cls="text-center")
                                    th("comment", cls="text-left")
                                    th(cls="text-left")
                            with tbody():
                                with tr(cls="form-group"):
                                    with td(cls="text-center"):
                                        input(id="ts", name="ts", style="width: 12em;", type="text", placeholder="%Y-%m-%d %H:%M", cls="form-control input-md")
                                    with td(cls="text-center"):
                                        with select(id="type", name="type", cls="form-control"):
                                            option("data", value="data", selected="selected")
                                            option("boost", value="boost")
                                    with td(cls="text-center"):
                                        with select(id="router", name="router", cls="form-control"):
                                            option("nihonium", value="nihonium", selected="selected")
                                    with td(cls="text-center"):
                                        with select(id="host", name="host", cls="form-control"):
                                            option("canald", value="canald")
                                            option("terbium", value="terbium")
                                            option("lutetium", value="lutetium")
                                            option("switch", value="switch")
                                            option("psvita", value="pspsm")
                                            option("hafnium", value="hafnium")
                                            option("neodym", value="neodym")
                                    with td(cls="text-center"):
                                        input(id="amount", name="amount", style="width: 8em;", type="text", placeholder="GiB/MiB/KiB", cls="form-control input-md")
                                    with td(cls="text-left"):
                                        input(id="comment", name="comment", type="text", placeholder="some text", cls="form-control input-md")
                                    with td():
                                        button("add", id="add", name="add", cls="btn btn-primary")
                                for add_data in data_list:
                                    with tr():
                                        cksum = hashlib.md5(repr(add_data).encode('utf-8')).hexdigest()
                                        td(add_data.ts.strftime('%Y-%m-%d %H:%M'), cls="text-center")
                                        td(add_data.atype, cls="text-center")
                                        td(add_data.router, cls="text-center")
                                        td(add_data.host if add_data.host is not None else '', cls="text-center")
                                        if add_data.atype == 'boost':
                                            td(str(int(add_data.amount.seconds / 3600)))
                                        else: td(bytes2units(add_data.amount))
                                        td(add_data.comment if add_data.comment is not None else '', cls="text-left")
                                        with td(cls="text-center"):
                                            button("del", id="del_%s" % cksum, name="del_%s" % cksum, cls="btn btn-primary")

print("Content-type: text/html")
print()
print(doc)
