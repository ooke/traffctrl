import sys, time, typing
import protocols as p
from datetime import datetime, timedelta
from utils import *
from typing import *

class AccountsReport(object):
    _limit_names: Tuple[str, ...]
    _accounts: Tuple[p.Account, ...]
    _storage: p.Storage
    
    def __init__(self,
                 limit_names: Tuple[str, ...],
                 accounts: Tuple[p.Account, ...],
                 storage: p.Storage):
        self._limit_names = limit_names
        self._accounts = accounts
        self._storage = storage

    def limits(self) -> Set[p.Limit]:
        result: Dict[p.Limit, Dict[p.Account, p.Usage]] = {}
        limits_set = set()
        for acc in self._accounts:
            for lname in acc.limit.limit_names:
                limits_set.add(acc.limit.limit(lname))
        return limits_set

    def account_usage(self, start_ts: datetime, router: str, limit_name: str) -> Dict[p.Account, p.Usage]:
        result: Dict[p.Account, p.Usage] = {}
        accounts_dict: Dict[str, p.Account] = {}
        cases_list: List[str] = []
        period: timedelta
        for account in self._accounts:
            if limit_name not in account.limit.limit_names:
                continue
            for host in account.hosts:
                accounts_dict[account.short] = account
                cases_list.append("WHEN host = '%s' THEN '%s'" % (host.name, account.short))
            period = account.limit.period(limit_name)
        if len(cases_list) > 0:
            the_cases = 'CASE %s ELSE NULL END' % ' '.join(cases_list)
            for usage in self._storage.sum(start_ts, start_ts - period,
                                           "router = '%s' GROUP BY (%s)" % (router, the_cases),
                                           reference_column = '%s AS account' % the_cases):
                if usage.ref is None: continue
                account = accounts_dict[usage.ref]
                result[account] = usage
        return result

    def host_usage(self, start_ts: datetime, router: str, limit_name: str) -> Dict[p.Host, p.Usage]:
        result: Dict[p.Host, p.Usage] = {}
        hosts_dict: Dict[str, p.Host] = {}
        host_list = []
        period: timedelta
        for account in self._accounts:
            if limit_name not in account.limit.limit_names:
                continue
            for host in account.hosts:
                host_list.append("host = '%s'" % host.name)
                hosts_dict[host.name] = host
            period = account.limit.period(limit_name)
        if len(host_list) > 0:
            host_filter = ' OR '.join(host_list)
            for host_usage in self._storage.sum(start_ts, start_ts - period,
                                                "(%s) AND router = '%s' GROUP BY host" % (host_filter, router)):
                if host_usage.ref is None:
                    raise ValueError('One of the hosts is not found.')
                result[hosts_dict[host_usage.ref]] = host_usage
        return result

    def account_usage_periodic(self, start_ts: datetime, router: str, days: int, period: str = 'day') -> Dict[str, Dict[p.Account, p.Usage]]:
        result: Dict[str, Dict[p.Account, p.Usage]] = {}
        accounts_dict: Dict[str, p.Account] = {}
        cases_list: List[str] = []
        for account in self._accounts:
            for host in account.hosts:
                accounts_dict[account.short] = account
                cases_list.append("WHEN host = '%s' THEN '%s'" % (host.name, account.short))
        if len(cases_list) > 0:
            if period == 'year': day_exp = "'' || year"
            elif period == 'month': day_exp = "'' || year || '-' || month"
            elif period == 'day': day_exp = "'' || year || '-' || month || '-' || day"
            elif period == 'hour': day_exp = "'' || year || '-' || month || '-' || day || '-' || hour"
            else: raise ValueError('Wrong period %s, need to be one of: year, month, day, hour.' % repr(period))
            the_cases = "(CASE %s ELSE NULL END || ' ' || %s)" % (' '.join(cases_list), day_exp)
            for usage in self._storage.sum(start_ts, start_ts - timedelta(days = days),
                                           "router = '%s' GROUP BY (%s), (%s) ORDER BY ref" \
                                           % (router, the_cases, day_exp),
                                           reference_column = '%s AS ref' % the_cases):
                if usage.ref is None: continue
                ref_account, ref_orig = usage.ref.split(' ')
                ref_tuple = tuple((int(x) for x in ref_orig.split('-')))
                if len(ref_tuple) == 1: ref_rest = cast(Tuple[int, ...], (1,1,0,0))
                elif len(ref_tuple) == 2: ref_rest = cast(Tuple[int, ...], (1,0,0))
                else: ref_rest = cast(Tuple[int, ...], (0,0))
                ref_ts = datetime(*ref_tuple, *ref_rest).strftime("%Y-%m-%d %H:00") # type: ignore
                if ref_ts not in result:
                    result[ref_ts] = {}
                account = accounts_dict[ref_account]
                result[ref_ts][account] = usage
        return result

class HtmlReport(object):
    def __init__(self,
                 router: str,
                 limit_names: Tuple[str, ...],
                 accounts: Tuple[p.Account, ...],
                 account_usage: Dict[str, Dict[p.Account, p.Usage]],
                 host_usage: Dict[str, Dict[p.Host, p.Usage]],
                 account_usage_daily: Dict[str, Dict[p.Account, p.Usage]],
                 account_usage_hourly: Dict[str, Dict[p.Account, p.Usage]],
                 footer: str = '',
                 output_stream: typing.TextIO = sys.stdout) -> None:
        self._router = router
        self._limit_names = limit_names
        self._accounts = accounts
        self._account_usage = account_usage
        self._host_usage = host_usage
        self._account_usage_daily = account_usage_daily
        self._account_usage_hourly = account_usage_hourly
        self._output_stream = output_stream
        self._footers: List[str] = [footer]

    def header(self) -> None:
        print("""<!DOCTYPE html>
<html><head><title>Internet usage %s</title>
<meta name="viewport" content="width=device-width, initial-scale=0.7">
<meta charset="UTF-8">
<link rel="stylesheet" href="./morris/morris.css">
<script src="./morris/jquery.min.js"></script>
<script src="./morris/raphael-min.js"></script>
<script src="./morris/morris.min.js"></script>
<style>
.smallblock {
  display: inline-block;
  padding: 5px;
  margin-right: 10px;
}
.bigblock {
  display: block;
  padding: 5px;
  margin-right: 10px;
}
.midblock {
  display: inline-block;
  padding: 5px;
  margin-right: 10px;
}
.tright {
  text-align: right;
}
</style></head><body>""" % self._router, file = self._output_stream)

    def footer(self) -> None:
        for line in self._footers:
            print(line, file = self._output_stream)
        print("""</body></html>""", file = self._output_stream)

    def _print_limit_block(self, limit: str) -> None:
        print("<table>", file = self._output_stream)
        dat_sum, dat_full = 0, 0
        for account in sorted(self._accounts, key = lambda x: x.short):
            if limit not in self._account_usage or account not in self._account_usage[limit]:
                usage = p.Usage(account.short)
            else: usage = self._account_usage[limit][account]
            print('<tr><th class="tright user%s">%s</th><td class="tright" title="%s / %s / %s">%s</td>' \
                  % (account.short, account.name.replace(' ', '&nbsp;'),
                     bytes2units(usage.inp), bytes2units(usage.out),
                     bytes2units(usage.inp + usage.out),
                     bytes2units(usage.dat)),
                  file = self._output_stream)
            percent = usage.dat / account.limit(limit).amount * 100
            red, yellow, weight = 0, 0, 'normal'
            if percent > 50.: red = yellow = min(int(percent / 100 * 150 + 50), 255)
            if percent > 80.: red = min(int(percent / 100 * 150 + 105), 255)
            if percent > 120.: weight = 'bold'
            print('<td class="tright" style="color: rgb(%d, %d, 0); font-weight: %s;">%s</td></tr>' \
                  % (red, yellow, weight, "%2.0f%%" % percent),
                  file = self._output_stream)
            if not account.ignore:
                dat_sum += usage.dat
                dat_full += usage.inp + usage.out
        print('<tr><th class="tright">%s</th><td class="tright" title="%s">%s</td><td>&nbsp;</td></tr></table>' \
              % ("SUM", bytes2units(dat_full), bytes2units(dat_sum)),
              file = self._output_stream)
    
    def accounts_usage(self) -> None:
        print('<div class="bigblock">', file = self._output_stream)
        print("<h1> INTERNET USAGE </h1>", file = self._output_stream)
        print("<p>%s</p>" % time.asctime(), file = self._output_stream)
        for limit_name in self._limit_names:
            print('<div class="smallblock"><h2>%s</h2>' % limit_name, file = self._output_stream)
            self._print_limit_block(limit_name)
            print("</div>", file = self._output_stream)
        print("</div>", file = self._output_stream)

    def hosts_usage(self) -> None:
        print('<div class="midblock"><h1>HOSTS</h1><table><tr><th>&nbsp;</th>',
              file = self._output_stream)
        for limit in self._limit_names:
            print('<th class="tright">%s</th>' % limit, end = ' ',
                  file = self._output_stream)
        print("</tr>", file = self._output_stream)
        for account in sorted(self._accounts, key = lambda x: x.short):
            for host in sorted(account.hosts, key = lambda x: x.name):
                print('<tr><th class="tright user%s">%s</th>' \
                      % (account.short, "%s/%s" % (host.name, account.short)),
                      end = ' ', file = self._output_stream)
                for limit_name in self._limit_names:
                    if limit_name not in self._host_usage \
                       or host not in self._host_usage[limit_name]:
                        usage = p.Usage(account.short)
                    else: usage = self._host_usage[limit_name][host]
                    print('<td class="tright" title="%s / %s / %s">%s</td>' % \
                          (bytes2units(usage.inp), bytes2units(usage.out),
                           bytes2units(usage.inp + usage.out),
                           bytes2units(usage.dat)),
                          file = self._output_stream)
                print("</tr>", file = self._output_stream)
        print("</table></div>", file = self._output_stream)

    def limits_usage(self) -> None:
        print('<div class="midblock"><h1>LIMITS</h1><table><tr><th>&nbsp;</th>',
              file = self._output_stream)
        sum_usage: Dict[str, p.Usage] = {}
        sum_limits: Dict[str, p.Usage] = {}
        for limit in sorted(self._limit_names):
            print('<th class="tright">%s</th>' % limit.replace(' ', '&nbsp;'),
                  end = ' ', file = self._output_stream)
            sum_usage[limit] = p.Usage(limit)
            sum_limits[limit] = p.Usage(limit)
        for account in sorted(self._accounts, key = lambda x: x.short):
            print('<tr><th class="tright user%s">%s</th>' \
                  % (account.short, account.name.replace(' ', '&nbsp;')),
                  end = ' ', file = self._output_stream)
            for limit in sorted(self._limit_names):
                if limit not in self._account_usage \
                   or account not in self._account_usage[limit]:
                    usage = p.Usage(account.short)
                else: usage = self._account_usage[limit][account]
                print('<td class="tright" title="%s / %s / %s">%s</td>' \
                      % (bytes2units(usage.inp), bytes2units(usage.out),
                         bytes2units(usage.inp + usage.out),
                         bytes2units(usage.dat)),
                      end = ' ', file = self._output_stream)
                if not account.ignore:
                    sum_usage[limit] += self._account_usage[limit][account]
                sum_limits[limit] += p.Usage('tmp', dat = account.limit(limit).amount)
            print("</tr>", file = self._output_stream)
        print('<tr><th class="tright">SUM</th>', end = ' ', file = self._output_stream)
        for _, usage in sorted(sum_limits.items(), key = lambda x: x[0]):
            print('<th class="tright">%s</th>' % bytes2units(usage.dat),
                  end = ' ', file = self._output_stream)
        print('</tr><tr><th class="tright">ADDS</th>', end = ' ', file = self._output_stream)
        for _, usage in sorted(sum_usage.items(), key = lambda x: x[0]):
            print('<th class="tright">%s</th>' \
                  % bytes2units((usage.inp + usage.out) - usage.dat),
                  end = ' ', file = self._output_stream)
        print('</tr><tr><th class="tright">USED</th>', end = ' ', file = self._output_stream)
        for _, usage in sorted(sum_usage.items(), key = lambda x: x[0]):
            print('<th class="tright" title="%s / %s / %s">%s</th>' \
                  % (bytes2units(usage.inp), bytes2units(usage.out),
                     bytes2units(usage.inp + usage.out),
                     bytes2units(usage.dat)),
                  end = ' ', file = self._output_stream)
        print("</tr></table></div>", end = ' ', file = self._output_stream)

    def charts(self) -> None:
        print("""
<div class="bigblock"><h1>DETAILS</h1>
<span style="padding: 10px;">daily:
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_daily, 92, 1);">92&nbsp;days</button>
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_daily, 62, 1);">62&nbsp;days</button>
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_daily, 32, 1);">32&nbsp;days</button>
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_daily, 16, 1);">16&nbsp;days</button>
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_daily,  8, 1);">8&nbsp;days</button>
</span>
<span>&nbsp;&nbsp;&nbsp;</span>
<span style="padding: 10px;">hourly:
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_hourly, 14, 24);">14&nbsp;days</button>
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_hourly, 7, 24);">7&nbsp;days</button>
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_hourly, 4, 24);">4&nbsp;days</button>
<button style="background: #ddd; padding: 5px;" onclick="draw_chart(usage_hourly, 2, 24);">2&nbsp;days</button>
</span>
<span>&nbsp;&nbsp;&nbsp;</span>
<span style="padding: 10px;">days:
<input type="text" id="days" value="92" size="3" onChange="draw_chart(null, null, null);"></input>
<span>&nbsp;&nbsp;</span>
<span style="padding: 10px;">offset:
<input type="text" id="offset" value="0" size="3" onChange="draw_chart(null, null, null);"></input>
</span>
<div id="chart1" style="height: 400px;"></div>
</div>""", file = self._output_stream)
        print("<script>", file = self._output_stream)
        print("var usage_daily = [", file = self._output_stream)
        for day_str, day_usage in sorted(self._account_usage_daily.items(), key = lambda x: x[0]):
            print("""{"date":"%s",""" % day_str, end = ' ', file = self._output_stream)
            for account, usage in sorted(day_usage.items(), key = lambda x: x[0].short):
                print(""""%s":%d,""" % (account.short, usage.dat / MiB),
                      end = ' ', file = self._output_stream)
            print("},", file = self._output_stream)
        print("];", file = self._output_stream)
        print("var usage_hourly = [", file = self._output_stream)
        for hour_str, hour_usage in sorted(self._account_usage_hourly.items(), key = lambda x: x[0]):
            print("""{"date":"%s",""" % hour_str, end = ' ', file = self._output_stream)
            for account, usage in sorted(hour_usage.items(), key = lambda x: x[0].short):
                print(""""%s":%d,""" % (account.short, usage.dat / MiB),
                      end = ' ', file = self._output_stream)
            print("},", file = self._output_stream)
        print("];", file = self._output_stream)
        print("""// setup chart1
var chart1 = {chart: null, data: null, mult: 1};
function draw_chart(data, days, mult, offset) {
  if (data == null) data = chart1.data;
  if (mult == null) mult = chart1.mult;
  if (days == null) days = Number(document.getElementById('days').value);
  if (offset == null) offset = Number(document.getElementById('offset').value);
  var start = data.length-(days*mult)-(offset*mult);
  var end = data.length-(offset*mult);
  if (start < 0) start = 0;
  if (end > data.length) end = data.length;
  if (chart1.chart == null) {
    chart1.chart = Morris.Line({ element: 'chart1', data: data.slice(start, end),
                                 xkey: 'date', postUnits: ' MiB', hideHover: true,
                                 ykeys: %s, labels: %s, pointSize: 0, resize: true});""" % \
              ([x.short for x in sorted(self._accounts, key = lambda x: x.short)],
               [x.name for x in sorted(self._accounts, key = lambda x: x.short)]),
              file = self._output_stream)
        print("""
  } else {
    chart1.chart.setData(data.slice(start, end));
  }
  chart1.data = data;
  chart1.mult = mult;
  document.getElementById('days').value = days;
  document.getElementById('offset').value = offset;
}""", file = self._output_stream)
        print("""// display aware usage data cut
var window_width = window.innerWidth * window.devicePixelRatio;
if (window_width >= 3000) { draw_chart(usage_daily, 92, 1); }
else if (window_width >= 1900) { draw_chart(usage_daily, 62, 1); }
else if (window_width >= 1800) { draw_chart(usage_daily, 32, 1); }
else if (window_width >= 750) { draw_chart(usage_daily, 16, 1); }
else { draw_chart(usage_daily, 9, null); }
""", file = self._output_stream)
        print("</script>", file = self._output_stream)

    def __call__(self) -> None:
        self.header()
        self.accounts_usage()
        self.hosts_usage()
        self.limits_usage()
        self.charts()
        self.footer()
