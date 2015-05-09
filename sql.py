#!/bin/env python

import psycopg2

# default values
class SQL:
    user = "user"
    pwd = "123456"
    host = "localhost"
    db = "data"
    hist_table = "historicaldata"
    create_hist_table = """
create table if not exists historicaldata (
    symbol varchar(10),
    date date,
    volume integer,
    low numeric,
    high numeric,
    close numeric,
    open numeric,
    adj_close numeric
);
"""

class sql_sink:
    def __init__(self, dbname, host, user, pwd, table=SQL.hist_table):
        self.conn_str_ = "dbname=%s user=%s password=%s" % (dbname, user, pwd)
        self.table_ = table
    def log_hist(self, data, filter=None):
        entries = []
        for symbol in data:
            for d in data[symbol].items():
                if filter is None or filter(d):
                    entries.append(d.values())

        try:
            conn = psycopg2.connect(self.conn_str_)
            cur = conn.cursor()
            
            # create table if doesn't exist
            cur.execute(SQL.create_hist_table)

            # insert data            
            sql = "insert into %s values" % self.table_
            cur.executemany(sql + " (%s,%s,%s,%s,%s,%s,%s,%s)", entries)
            
            conn.commit()
            cur.close()
            conn.close()           
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            import traceback
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            print("Failed to execute SQL query.")        
