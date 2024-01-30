# can we fix the negative "sentinel" values 
# which come in the new table-based 
# ACS Detailed table data
# Census calls them "Jam Values" and documents them in
# a per-release page linked from https://www.census.gov/programs-surveys/acs/technical-documentation/code-lists.html
from flask import Flask
import os, sys
import csv
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.engine import Connection

# realistically, any value lower than -100000000 is a jam value
JAM_VALUES = [
    -222222222,
    -333333333, # -333333340.0
    -555555555,
    -666666666,
    -888888888,
    -999999999
]

def fix_column(cur: Connection, column_id: str, table_id: str):
    for schema in ['acs2022_5yr', 'acs2022_1yr']:
        sql = f"""update {schema}.{table_id}_moe set {column_id} = null where {column_id} < -100000000"""
        try:
            cur.execute(sql)
        except Exception as e:
            print(f"{schema}.{table_id} {column_id} {e}")

def main(input_file):
    app = Flask(__name__)
    app.config.from_object(os.environ.get('EXTRACTOMATIC_CONFIG_MODULE', 'census_extractomatic.config.Development'))
    db = SQLAlchemy(app)
    reader = csv.DictReader(input_file) # table,column,value,count
    columns_done = set()

    for i,row in enumerate(reader):
        if not row['column'] in columns_done and float(row['value']) <= -100000000:
            fix_column(db.engine, row['column'], row['table'])
            columns_done.add(row['column'])
            if i > 0 and i % 100 == 0:
                print(f"{i:05} {row['column']}")
    print(f"done {len(columns_done)} columns")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Specify the CSV filename as first argument")
        sys.exit(1) 
    else:
        print(f"reading {sys.argv[1]}")
        with open(sys.argv[1]) as input_file:
            main(input_file)
