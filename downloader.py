#!/bin/env python

import time
import optparse
from sql import SQL, sql_sink
from common import * 

class driver:
    no_data_error = "No data for: %s"
    request_failed_error = "Failed to request data for: %s"
    
    def __init__(self, file, options):
        self.file_ = file
        self.options_ = options
        self.request_ = request(diag=False)
        self.symbols_ = []
        self.errors_ = {}

    def read(self):
        try:
            f = open(self.file_, 'r')
            for symbol in f:
                symbol = symbol.strip()
                symbol = symbol[:-1] if symbol[-1:] == '.' else symbol 
                self.symbols_.append("%s.L" % symbol)
            f.close()
            return True
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            import traceback
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            return False
    
    def fetch(self):
        if not self.read():
            return False

        self.errors_ = {}        
        self.data_ = {}
        
        count = 0        
        for symbol in self.symbols_:
            if self.request_.prepare_hist(symbol, self.options_.start, self.options_.end):
                r = self.request_.send()
                if r is not None:
                    for e in r:
                        k = e["Symbol"]
                        if k not in self.data_:
                            self.data_[k] = hist_price_list(k)
                        self.data_[e["Symbol"]].add(hist_price_data(e))
                else:
                    self.errors_[symbol] = driver.no_data_error % symbol
            else:
                self.errors_[symbol] = driver.request_failed_error % symbol
            count += 1
            if count == 10:
                time.sleep(1)
        return True

    def errors(self):
        return self.errors_
    def data(self):
        return self.data_

if __name__ == "__main__":
    usage = "usage: %prog [options] <file with stocks>"
    opt = optparse.OptionParser(usage=usage, version="%prog 0.1")
    opt.add_option("-s", dest="start", default=None, help="start date")
    opt.add_option("-e", dest="end", default=None, help="end date")
    opt.add_option("--sql", dest="sql", action="store_true", default=False, help="output to sql")
    opt.add_option("-u", dest="user", default=SQL.user, help="sql username")
    opt.add_option("-p", dest="pwd", default=SQL.pwd, help="sql password")
    opt.add_option("--host", dest="host", default=SQL.host, help="sql host")
    opt.add_option("-d", dest="db", default=SQL.db, help="sql database")
    
    try:
        (options, args) = opt.parse_args()
        
        if len(args) == 0:
            opt.error("stocks file is not specified")
        
        s = driver(args[0], options)
        
        if s.fetch():
            if options.sql:
                out = sql_sink(options.db, options.host, options.user, options.pwd)
            else:
                out = console_sink()
            out.log_hist(s.data())
            
        for k in s.errors():
            print(s.errors()[k])

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        import traceback
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        sys.exit(1)

    sys.exit(0)
