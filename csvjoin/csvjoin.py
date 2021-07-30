#!/usr/bin/env python3
# Yonghang Wang

import argparse
import os
import sys
import string
import random
import pandas
from xtable import xtable
import traceback
import sqlite3


def _csvjoin_main():
    parser = argparse.ArgumentParser(description="CSV query in SQL. Yonghang Wang, wyhang@gmail.com, 2021")
    parser.add_argument( "-t", "--table", dest="tables", action="append",default=list(), help="specify csv files. '[alias=]csvfile'")
    parser.add_argument( "-i", "--index", dest="indexes", action="append",default=list(), help="index. tbl(c1,c2,...)")
    parser.add_argument( "-d", "--db", "--database",dest="db", default=":memory:",  help="database name. default in memory.")
    parser.add_argument( "-q", "--sql", "--query",dest="sql", default=None,  help="SQL stmt or file containing sql query")
    parser.add_argument( "-a", "--adhoc", dest="adhoc", action="append", default=list(),help="adhoc DDL/DML such as view full definition.")
    parser.add_argument( "-X", "--debug", dest="debug", action="store_true", default=False, help="debug mode",)
    parser.add_argument( "--json", dest="json", action="store_true", default=False, help="dump result in JSON",)
    parser.add_argument( "--csv", dest="csv", action="store_true", default=False, help="dump result in CSV",)
    parser.add_argument( "--html", dest="html", action="store_true", default=False, help="dump result in HTML",)
    parser.add_argument( "--markdown", dest="markdown", action="store_true", default=False, help="dump result in Markdown",)
    parser.add_argument( "--table-creation-mode", dest="tablemode", default="append", help="if_exists{fail,replace,append}, default 'append'",)
    args = parser.parse_args()
    
    def _x(s) :
        if args.debug :
            print("# "+s,file=sys.stderr,flush=True)

    con = sqlite3.connect(args.db)
    cur = None

    for csvfile in args.tables :
        if "=" in csvfile :
            tbname,csvfile = csvfile.split("=",maxsplit=1)
        else :
            tbname = "_".join(csvfile.split(".")[:-1])
        _x("loading table {} from {}".format(tbname,csvfile))
        df = pandas.read_csv(os.path.expanduser(csvfile)) 
        df.to_sql(tbname, con, if_exists=args.tablemode, index=False)
        con.commit()

    def randname(n) :
        m = max(n,3)
        return "_ix_" + "".join([random.choice(string.ascii_lowercase) for _ in range(m)])

    for idx in args.indexes :
        stmt = "create index {} on ".format(randname(5))  + idx
        if not cur :
            cur = con.cursor()
        _x(stmt)
        cur.execute(stmt)
        con.commit()

    for vstmt in args.adhoc :
        if not cur :
            cur = con.cursor()
        _x(vstmt)
        cur.execute(vstmt)
        con.commit()

    if args.sql :
        sql = args.sql
        if os.path.isfile(sql) :
            _x("loading query from {}".format(sql))
            sql = open(sql,"r").read()
    
        _x("run : {}".format(sql))
        df = pandas.read_sql_query(sql, con)
    
        if args.json :
            print(df.to_json(orient="records"),flush=True)
            return
        elif args.csv :
            print(df.to_csv(index=None),flush=True)
            return
        elif args.html :
            print(df.to_html(index=False),flush=True)
            return
        elif args.markdown:
            print(df.to_markdown(index=False),flush=True)
            return
        else :
            if df.empty :
                print("# empty set.",file=sys.stderr,flush=True)
                return
            print(xtable(data=df.values.tolist(),header=list(df.keys())))

def csvjoin_main():
    try :
        _csvjoin_main()
    except :
        exitinfo = traceback.format_exc().splitlines()[-1:]
        if "SystemExit: 0" not in exitinfo :
            print("\n".join(exitinfo),file=sys.stderr,flush=True)

if __name__ == "__main__":
    csvjoin_main()
