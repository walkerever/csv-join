#!/usr/bin/env python3
# Yonghang Wang

import argparse
import os
import sys
import string
import random
import pandas
import re
from xtable import xtable
from sqlalchemy import create_engine
import traceback
import sqlite3


def csvjoin_main():
    parser = argparse.ArgumentParser(description="CSV query in SQL. Yonghang Wang, wyhang@gmail.com, 2021")
    parser.add_argument( "-t", "--table", dest="tables", action="append",default=list(), help="specify csv files. '[alias=]csvfile'")
    parser.add_argument( "-i", "--index", dest="indexes", action="append",default=list(), help="index. tbl(c1,c2,...)")
    parser.add_argument( "-d", "--db", "--database","--engine",dest="db", default=":memory:",  help="database name. default sqlite in memory. use full sqlalchedmy url for other dbms.")
    parser.add_argument( "-q", "--sql", "--query",dest="sql", default=None,  help="SQL stmt or file containing sql query")
    parser.add_argument( "-b", "--delimiter",dest="sep", default=',',  help="csv delimiter")
    parser.add_argument( "-a", "--adhoc", dest="adhoc", action="append", default=list(),help="adhoc DDL/DML such as view full definition.")
    parser.add_argument( "-X", "--debug", dest="debug", action="store_true", default=False, help="debug mode",)
    parser.add_argument( "--json", dest="json", action="store_true", default=False, help="dump result in JSON",)
    parser.add_argument( "--csv", dest="csv", action="store_true", default=False, help="dump result in CSV",)
    parser.add_argument( "--html", dest="html", action="store_true", default=False, help="dump result in HTML",)
    parser.add_argument( "--markdown", dest="markdown", action="store_true", default=False, help="dump result in Markdown",)
    parser.add_argument( "--pivot", dest="pivot", action="store_true", default=False, help="pivot the result. better for wide table.",)
    parser.add_argument( "--table-creation-mode", dest="tablemode", default="append", help="if_exists{fail,replace,append}, default 'append'",)
    args = parser.parse_args()
    
    def _x(s) :
        if args.debug :
            print("# "+s,file=sys.stderr,flush=True)

    def randname(n) :
        m = max(n,3)
        return "_ix_" + "".join([random.choice(string.ascii_lowercase) for _ in range(m)])

    if "//" in args.db :
        try :
            engine = create_engine(args.db)
            con = engine.connect()
        except :
            print(traceback.format_exc().splitlines()[-1],file=sys.stderr,flush=True)
            sys.exit(-1)
    else :
        try :
            con = sqlite3.connect(args.db)
        except :
            print(traceback.format_exc().splitlines()[-1],file=sys.stderr,flush=True)
            sys.exit(-1)
    cur = None

    for csvfile in args.tables :
        if "=" in csvfile :
            tbname,csvfile = csvfile.split("=",maxsplit=1)
        else :
            tbname = "_".join(csvfile.split(".")[:-1])
        _x("loading table {} from {}".format(tbname,csvfile))
        df = pandas.read_csv(os.path.expanduser(csvfile),sep=args.sep) 
        try :
            df.to_sql(tbname, con, if_exists=args.tablemode, index=False)
        except :
            print(traceback.format_exc().splitlines()[-1],file=sys.stderr,flush=True)
            con.close()
            sys.exit(-1)
        try :
            con.commit()
        except :
            pass

    for idx in args.indexes :
        stmt = "create index {} on ".format(randname(5))  + idx
        if not cur :
            cur = con.cursor()
        _x(stmt)
        try :
            cur.execute(stmt)
        except :
            print(traceback.format_exc().splitlines()[-1],file=sys.stderr,flush=True)
            con.close()
            sys.exit(-1)
        try :
            con.commit()
        except :
            pass

    for vstmt in args.adhoc :
        if not cur :
            cur = con.cursor()
        _x(vstmt)
        try :
            cur.execute(vstmt)
        except :
            print(traceback.format_exc().splitlines()[-1],file=sys.stderr,flush=True)
            con.close()
            sys.exit(-1)
        try :
            con.commit()
        except :
            pass

    if args.sql :
        sql = args.sql
        if os.path.isfile(sql) :
            _x("loading query from {}".format(sql))
            sql = open(sql,"r").read()
        _x("query = {}".format(sql))
        if not (re.search(r"^\s*select\s+",sql,re.IGNORECASE) or re.search(r"^\s*with\s+",sql,re.IGNORECASE) or re.search(r"^\s*values(\s+|\()",sql,re.IGNORECASE)) :
            print("# not a valid query : {}".format(sql),file=sys.stderr,flush=True)
            print("# for non query ddl/dml, use adhoc(-a) option explicitly.",file=sys.stderr,flush=True)
            if con :
                con.close()
            sys.exit(-1)

        if (re.search(r"\bmerge\b",sql,re.IGNORECASE) or re.search(r"\bupdate\b",sql,re.IGNORECASE) or re.search(r"\bdelete\b",sql,re.IGNORECASE) ) :
            print("# Warn : for data change without resultset, adhoc(-a) is a better option",file=sys.stderr,flush=True)

        try :
            df = pandas.read_sql_query(sql, con)
        except :
            print(traceback.format_exc().splitlines()[-1],file=sys.stderr,flush=True)
            con.close()
            sys.exit(-1)
        if args.json :
            print(df.to_json(orient="records"),flush=True)
        elif args.csv :
            print(df.to_csv(index=None),flush=True)
        elif args.html :
            print(df.to_html(index=False),flush=True)
        elif args.markdown:
            print(df.to_markdown(index=False),flush=True)
        else :
            if df.empty :
                print("# empty set.",file=sys.stderr,flush=True)
            else :
                xt = xtable(data=df.values.tolist(),header=list(df.keys()))
                if args.pivot :
                    print(xt.pivot())
                else :
                    print(xt)
    con.close()


if __name__ == "__main__":
    csvjoin_main()
