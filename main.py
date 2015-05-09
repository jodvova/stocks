#!/bin/env python

import optparse
from sql import SQL, sql_sink
from common import * 

class driver:
    def __init__(self, symbols, options):
        self.symbols_ = symbols
        self.options_ = options
        self.request_ = request(diag=False)
    
    def fetch_live(self):
        assert len(self.symbols_) > 0
        self.data_ = []
        
        if self.request_.prepare_live(self.symbols_):
            r = self.request_.send()
            if r is not None:
                if isinstance(r, list):
                    for e in r: self.data_.append(live_data(e))
                else:
                    self.data_.append(live_data(r))
            else:
                print("No live prices received for: %s" % str(self.symbols_))
    
    def fetch_hist(self):
        assert len(self.symbols_) > 0
        self.data_ = {}
        
        if self.request_.prepare_hist(self.symbols_, self.options_.start, self.options_.end):
            r = self.request_.send()
            if r is not None:
                for e in r:
                    k = e["Symbol"]
                    if k not in self.data_:
                        self.data_[k] = hist_price_list(k)
                    self.data_[e["Symbol"]].add(hist_price_data(e))
            else:
                print("No historical prices received for: %s\n" % str(self.symbols_))
    
    def data(self):
        return self.data_

if __name__ == "__main__":
    usage = "usage: %prog [options] <symbol1> [<symbol2>..<symbolN>]"
    opt = optparse.OptionParser(usage=usage, version="%prog 0.1")
    opt.add_option("-s", dest="start", default=None, help="start date")
    opt.add_option("-e", dest="end", default=None, help="end date")
    opt.add_option("-i", dest="ignore_zero_vol", action="store_true", default=False, help="ignore 0 total volume")
    opt.add_option("--sql", dest="sql", action="store_true", default=False, help="output to sql")
    opt.add_option("-u", dest="user", default=SQL.user, help="sql username")
    opt.add_option("-p", dest="pwd", default=SQL.pwd, help="sql password")
    opt.add_option("--host", dest="host", default=SQL.host, help="sql host")
    opt.add_option("-d", dest="db", default=SQL.db, help="sql database")
    
    try:
        (options, args) = opt.parse_args()
        
        if len(args) == 0:
            opt.error("incorrect number of stocks")
        
        s = driver(args, options)
        if options.start is None or options.end is None:
            s.fetch_live()
            out = console_sink()
            out.log_live(s.data())
        else:
            s.fetch_hist()
            if options.sql:
                out = sql_sink(options.db, options.host, options.user, options.pwd)
            else:
                out = console_sink()
            out.log_hist(s.data(), lambda d: True if not options.ignore_zero_vol or d.volume() > 0 else False)

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        import traceback
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        sys.exit(1)

    sys.exit(0)
