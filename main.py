#!/bin/env python

import sys
import optparse
import requests

class request:
    parameters = {
        'q': '',
        'format': 'json',
        'diagnostics': 'false',
        'env': 'store://datatables.org/alltableswithkeys',
        'callback': ''
    }
    historical_table = "yahoo.finance.historicaldata"
    live_table = "yahoo.finance.quotes"
    
    api = 'https://query.yahooapis.com/v1/public/yql'
    
    def __init__(self, diag=False):
        self.query_ = None
        self.diag_ = diag
        
    def prepare_live(self, symbols):
        if isinstance(symbols, list):
            self.query_ = 'select * from %s where symbol in ("%s")' % (request.live_table, '","'.join(symbols))
        else:
            self.query_ = 'select * from %s where symbol = "%s"' % (request.live_table, symbols)
        return True
    
    def prepare_hist(self, symbols, start, end):
        if isinstance(symbols, list):
            self.query_ = 'select * from %s where symbol in ("%s") and startDate = "%s" and endDate = "%s"' % (request.historical_table, '","'.join(symbols), start, end)
        else:
            self.query_ = 'select * from %s where symbol = "%s" and startDate = "%s" and endDate = "%s"' % (request.historical_table, symbols, start, end)
        return True

    def send(self):
        self.parameters['q'] = self.query_
        self.parameters['diagnostics'] = str(self.diag_).lower()
        response = requests.get(self.api, params=self.parameters).json()
        if response['query']['count'] > 0:
            return response['query']['results']['quote']
        else:
            return None

class hist_price_data:
    def __init__(self, dict):
        self.dict_ = dict
    def symbol(self):
        return self.dict_["Symbol"]
    def date(self):
        return self.dict_["Date"]
    def volume(self):
        return int(self.dict_["Volume"])
    def low(self):
        return float(self.dict_["Low"])
    def high(self):
        return float(self.dict_["High"])
    def close(self):
        return float(self.dict_["Close"])
    def open(self):
        return float(self.dict_["Open"])
    def adj_close(self):
        return float(self.dict_["Adj_Close"])

class hist_price_list:
    def __init__(self, symbol):
        self.symbol_ = symbol
        # date -> price data
        self.data_map_ = {}
    def add(self, entry):
        assert entry.date() not in self.data_map_
        self.data_map_[entry.date()] = entry
    def items(self):
        return [self.data_map_[k] for k in sorted(self.data_map_.keys())]

class live_data:
    def __init__(self, dict):
        self.dict_ = dict
    def symbol(self):
        return self.dict_["Symbol"]
    def bid(self):
        return float(self.dict_["Bid"])
    def ask(self):
        return float(self.dict_["Ask"])
    def volume(self):
        return int(self.dict_["Volume"])
    def avg_volume(self):
        return int(self.dict_["AverageDailyVolume"])
    def last_trade_date(self):
        return self.dict_["LastTradeDate"]
    def change_pct(self):
        return self.dict_["ChangeinPercent"]
    def ex_div_date(self):
        return self.dict_["ExDividendDate"]
    def pe_ration(self):
        return self.dict_["PERatio"]
    def moving_avg_200(self):
        return float(self.dict_["TwoHundreddayMovingAverage"])
    def moving_avg_50(self):
        return float(self.dict_["FiftydayMovingAverage"])
        
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
                for e in r:
                    self.data_.append(live_data(e))
            else:
                print("No live prices received for: %s" % str(self.symbols_))
    
    def show_live(self):
        body = "symbol,bid,ask,volume,change_pct,200avg,50avg\n"
        for d in self.data_:
            body += "%s,%.2f,%.2f,%d,%s,%.2f,%.2f\n" % (d.symbol(), d.bid(), d.ask(), d.volume(), d.change_pct(), d.moving_avg_200(), d.moving_avg_50())
        print(body)

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
            
    def show_hist(self):
        body = "symbol,date,volume,high,low,close,open,aclose\n" 
        for n in self.data_:
            hdata = self.data_[n]
            for d in hdata.items():
                if d.volume() == 0 and self.options_.ignore_zero_vol:
                    continue
                body += "%s,%s,%d,%.2f,%.2f,%.2f,%.2f,%.2f\n" % (d.symbol(), d.date(), d.volume(), d.high(), d.low(), d.close(), d.open(), d.adj_close())
        print(body)

if __name__ == "__main__":
    usage = "usage: %prog [options] <symbol1> [<symbol2>..<symbolN>]"
    opt = optparse.OptionParser(usage=usage, version="%prog 0.1")
    opt.add_option("-s", dest="start", default=None, help="start date")
    opt.add_option("-e", dest="end", default=None, help="end date")
    opt.add_option("-i", dest="ignore_zero_vol", action="store_true", default=False, help="ignore 0 total volume")
    
    try:
        (options, args) = opt.parse_args()
        
        if len(args) == 0:
            opt.error("incorrect number of stocks")
        
        s = driver(args, options)
        if options.start is None or options.end is None:
            s.fetch_live()
            s.show_live()
        else:
            s.fetch_hist()
            s.show_hist()
            
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        import traceback
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        sys.exit(1)

    sys.exit(0)
