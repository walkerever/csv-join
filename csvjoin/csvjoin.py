#!/usr/bin/env python3
# Yonghang Wang

import argparse
import os
import sys
import pandas
import sqlite3

def csvjoin_main():
    parser = argparse.ArgumentParser()
    parser.add_argument( "-d", "--csv", "--data", dest="csv", action="append", help="specify csv files. '[alias=]csvfile'")
    parser.add_argument( "-q", "--sql", "--query",dest="sql", default=None,  help="SQL")
    parser.add_argument( "-J", "--json", dest="json", action="store_true", default=False, help="dump result in JSON",)
    parser.add_argument( "-X", "--debug", dest="debug", action="store_true", default=False, help="debug mode",)
    args = parser.parse_args()
    
    if not (args.csv and args.sql ) :
        print("# must specify csv files(-d) and sql(-q).",file=sys.stderr,flush=True)
        sys.exit(-1)

    con = sqlite3.connect(":memory:")

    for csvfile in args.csv :
        if "=" in csvfile :
            tbname,csvfile = csvfile.split("=",maxsplit=1)
        else :
            tbname = csvfile.replace(".csv","").replace(".","_")
        df = pandas.read_csv(os.path.expanduser(csvfile)) 
        df.to_sql(tbname, con, if_exists="replace")

    pandas.set_option("max_columns",None)
    pandas.set_option("max_rows",None)
    pandas.options.display.width = 0

    df = pandas.read_sql_query(args.sql, con)
    if args.json :
        print(df.to_json(orient="records"))
    else :
        print(df)


if __name__ == "__main__":
    csvjoin_main()
