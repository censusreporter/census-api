# Despite the name, this is more than testing. It forces the generation
# (and caching) of all of the files which people might wish to download, 
# thus avoiding the risk of a timeout when someone requests an uncached
# file from the public web interface.
# It's called "test" because it also stores timing data, should we ever
# want to assess that. It also uses that stored data to know which 
# user geo datasets even need processing, so this should only do "new" ones.
#
# In a census-api environment, with a database tunnel open, 
# run this with
# python -m census_extractomatic.aggregation.test
from census_extractomatic.user_geo import (
    create_aggregate_download,
    COMPARISON_RELEASE_CODE,
    create_block_xref_download
)
from timeit import default_timer as timer
from datetime import timedelta

import os
from sqlalchemy import create_engine
import csv
from pathlib import Path

SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')

engine = create_engine(SQLALCHEMY_DATABASE_URI)

def main():
    LOGFILE_PATH = Path(Path(__file__).parent,'timing.log')
    done = set()
    if os.path.isfile(LOGFILE_PATH):
        for row in csv.DictReader(open(LOGFILE_PATH)):
            done.add(row['hash_digest'])
    datasets = list(engine.execute("select hash_digest, name from aggregation.user_geodata where status = 'READY'"))
    with open(LOGFILE_PATH,"a+") as f:
        w = csv.DictWriter(f, ['hash_digest','name','table','release', 'elapsed_secs'])
        if len(done) == 0:
            w.writeheader()
        for hash_digest, name in datasets:
            
            if hash_digest not in done:
                row = {
                    'hash_digest': hash_digest, 
                    'name': name
                }
                # BLOCK ASSIGNMENT FILES
                for year in ['2020','2010']:
                    table_id = 'block_assignment'
                    row['table'] = table_id
                    row['release'] = year
                    print(f"{hash_digest} {table_id} {year} {name}")
                    start = timer()
                    tmp = create_block_xref_download(engine, hash_digest, year)
                    end = timer()
                    row['elapsed_secs'] = f"{timedelta(seconds=end-start)}"
                    w.writerow(row)

                # DECENNIAL AGGREGATIONS
                for table_id, release in [
                    ('P1', COMPARISON_RELEASE_CODE),
                    ('P2', COMPARISON_RELEASE_CODE),
                    ('P3', COMPARISON_RELEASE_CODE),
                    ('P4', COMPARISON_RELEASE_CODE),
                    ('P5', 'dec2020_pl94'),
                    ('H1', COMPARISON_RELEASE_CODE),
                ]:
                    row['table'] = table_id
                    row['release'] = release
                    print(f"{hash_digest} {table_id} {name}")
                    start = timer()
                    tmp = create_aggregate_download(engine, hash_digest, release, table_id)
                    end = timer()
                    row['elapsed_secs'] = f"{timedelta(seconds=end-start)}"
                    w.writerow(row)
    print("\n\nFinished\n\n")
        

if __name__ == '__main__':
    main()
