# GeoPandas is not a dependency for census-api because it is too heavy
# so write this to run without census-api code
import os
from sqlalchemy import create_engine, text
import geopandas as gpd
import pandas as pd
import csv
from pathlib import Path

def place_blocks2020(db,state,place):
    sql = text("""select g.geoid, g.hu100::integer, g.pop100::integer, g.state || g.place fips, b.geom
    from dec2020_pl94.geoheader g,
    blocks.tabblock20 b
    where place = :place and state = :state
    and g.geoid = b.geoid20 """).bindparams(state=state,place=place)
    return gpd.read_postgis(sql,db)

def cr_blocks2020(db,hash_digest):
    SQL = """
    select g.geoid, g.hu100::integer, g.pop100::integer, g.state || g.place fips, b.geom
    from dec2020_pl94.geoheader g,
    aggregation.user_geodata ug,
    aggregation.user_geodata_geometry ugg,
    aggregation.user_geodata_blocks_2020 ugb,
    blocks.tabblock20 b
    where ug.hash_digest = :hash_digest
    and ug.user_geodata_id = ugg.user_geodata_id
    and ugg.user_geodata_geometry_id = ugb.user_geodata_geometry_id
    and ugb.geoid = g.geoid
    and g.geoid = b.geoid20
    """
    return gpd.read_postgis(text(SQL).bindparams(hash_digest=hash_digest),db)

def create_compound(db, hash_digest, state, place):
    cenblocks = place_blocks2020(db, state, place)
    crblocks = cr_blocks2020(db, hash_digest)
    crblocks['status'] = crblocks['fips'].apply(lambda x: 'correct' if x == f'{state}{place}' else 'incorrect')
    missing = cenblocks[~(cenblocks['geoid'].isin(crblocks['geoid']))].copy()
    if not missing.empty:
        missing['status'] = missing.apply(lambda row: 'missing' if row['pop100'] > 0 or row['hu100'] > 0 else 'empty', axis=1)
        return pd.concat([crblocks,missing])
    return crblocks

def main():
    BASE_DIR = Path(__file__).parent
    block_audit_dir = Path(BASE_DIR,'block_audit')
    block_audit_csv_path = Path(block_audit_dir,'block_audit.csv')

    db = create_engine(os.environ['CR_PSQL_TUNNEL_URL'])

    os.makedirs(block_audit_dir,exist_ok=True)
    basis = list(csv.DictReader(open(Path(BASE_DIR,"audit_guide.csv"),'r')))
    fieldnames = ['user_geodata_id', 'hash_digest', 'name', 'state', 'place', 'pop100_agg', 'pop100_real', 'hu100_agg', 'hu100_real', 'missing_blocks', 'wrong_blocks' ]
    done = set(x.split('.')[0] for x in os.listdir(block_audit_dir) if x.endswith('.geojson'))
    appending = os.path.exists(block_audit_csv_path)
    with open(block_audit_csv_path,'a+') as f:
        w = csv.DictWriter(f,fieldnames=fieldnames)
        if not appending:
            w.writeheader()
        for row in basis:
            print(f"{row['hash_digest']} {row['name']} {row['hash_digest'] in done}")
            if row['hash_digest'] not in done:
                geojson_file_path = Path(block_audit_dir,f'{row["hash_digest"]}.geojson')
                combined = create_compound(db, row['hash_digest'], row['state'], row['place'])
                row['pop100_agg'] = combined[combined['status'].isin(['correct', 'incorrect'])]['pop100'].sum()
                row['hu100_agg'] = combined[combined['status'].isin(['correct', 'incorrect'])]['hu100'].sum()
                row['pop100_real'] = combined[combined['fips'] == f"{row['state']}{row['place']}"]['pop100'].sum()
                row['hu100_real'] = combined[combined['fips'] == f"{row['state']}{row['place']}"]['hu100'].sum()
                row['missing_blocks'] = len(combined[combined['status'] == 'missing'])
                row['wrong_blocks'] = len(combined[combined['status'] == 'incorrect'])
                w.writerow(row)
                combined.to_file(geojson_file_path, driver='GeoJSON')  
    print("done")
            
            
if __name__ == '__main__':
    main()
