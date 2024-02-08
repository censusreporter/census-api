"""Centralize non-Flask code for 2020 User Geography data aggregation here.
    This file serves both as a library for the Flask app as well as
    a bootstrap for Celery tasks, which could be run with something like
    celery -A census_extractomatic.user_geo:celery_app worker
"""
from datetime import timedelta
from sqlalchemy.sql import text
import json
from collections import OrderedDict
from copy import deepcopy

from tempfile import NamedTemporaryFile
import zipfile

import pandas as pd
import numpy as np

from osgeo import ogr

from celery import Celery
import os
from sqlalchemy import create_engine

import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger('gunicorn.error')

from timeit import default_timer as timer

SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
CELERY_BROKER = os.environ['REDIS_URL']

celery_app = Celery(__name__, broker=CELERY_BROKER)
celery_db = create_engine(SQLALCHEMY_DATABASE_URI)

@celery_app.task
def join_user_geo_to_blocks_task(user_geodata_id):
    join_user_to_census(celery_db, user_geodata_id)

COMPARISON_RELEASE_CODE = 'dec_pl94_compare_2020_2010'

USER_GEODATA_INSERT_SQL = text("""
INSERT INTO aggregation.user_geodata (name, hash_digest, source_url, public, fields, bbox)
VALUES (:name, :hash_digest, :source_url, :public, :fields, ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326))
RETURNING *
""")

USER_GEODATA_GEOMETRY_INSERT_SQL = text("""
INSERT INTO aggregation.user_geodata_geometry (user_geodata_id, geom, name, original_id, properties)
VALUES (:user_geodata_id, 
        ST_Transform(
            ST_GeomFromText(:geom_wkt,:epsg),
            4326),
        :name,
        :original_id,
        :properties
        )
""")

USER_GEODATA_SELECT_BY_HASH_DIGEST = text('''
SELECT user_geodata_id, 
       EXTRACT(EPOCH from created_at) unix_timestamp, 
       name, 
       bbox, 
       fields, 
       source_url,
       status,
       notes_html,
       public
FROM aggregation.user_geodata 
WHERE hash_digest=:hash_digest
''')



AGGREGATE_BLOCKS_2010_SQL = text("""
INSERT INTO aggregation.user_geodata_blocks_2010 (user_geodata_geometry_id, geoid)
SELECT ugg.user_geodata_geometry_id, b.geoid10
  FROM aggregation.user_geodata ug,
       aggregation.user_geodata_geometry ugg,
       blocks.tabblock10 b
 WHERE ug.user_geodata_id = :geodata_id
   AND ug.user_geodata_id = ugg.user_geodata_id
   AND ST_Intersects(ug.bbox, b.geom)
   AND ST_Contains(ugg.geom,
                   ST_SetSRID(ST_MakePoint(b.intptlon10::double precision, 
                                           b.intptlat10::double precision), 
                    4326))
""")

AGGREGATE_BLOCKS_2020_SQL = text("""
INSERT INTO aggregation.user_geodata_blocks_2020 (user_geodata_geometry_id, geoid)
SELECT ugg.user_geodata_geometry_id, b.geoid20
  FROM aggregation.user_geodata ug,
       aggregation.user_geodata_geometry ugg,
       blocks.tabblock20 b
 WHERE ug.user_geodata_id = :geodata_id
   AND ug.user_geodata_id = ugg.user_geodata_id
   AND ST_Intersects(ug.bbox, b.geom)
   AND ST_Contains(ugg.geom,
                   ST_SetSRID(ST_MakePoint(b.intptlon20::double precision, 
                                           b.intptlat20::double precision), 
                    4326))
""")

USER_GEOMETRY_SELECT_WITH_GEOM_BY_HASH_DIGEST = text('''
SELECT ugg.user_geodata_geometry_id, ugg.name, ugg.original_id, ST_asGeoJSON(ST_ForcePolygonCCW(ugg.geom))
FROM aggregation.user_geodata ug,
    aggregation.user_geodata_geometry ugg
WHERE ug.hash_digest=:hash_digest
  AND ug.user_geodata_id = ugg.user_geodata_id
''')

USER_GEOMETRY_SELECT_2020_BLOCKS_WITH_GEOM_BY_HASH_DIGEST = text('''
SELECT ug.name upload_name,
        ugb.geoid,
        ugg.user_geodata_geometry_id cr_geoid, 
        ugg.name, 
        ugg.original_id, 
		g.pop100,
		g.hu100,
		g.state || g.place as state_place_fips,        
        ST_asGeoJSON(ST_ForcePolygonCCW(b.geom)) geom
FROM aggregation.user_geodata ug,
    aggregation.user_geodata_geometry ugg,
    aggregation.user_geodata_blocks_2020 ugb,
    dec2020_pl94.geoheader g,
    blocks.tabblock20 b
WHERE ug.hash_digest=:hash_digest
  AND ug.user_geodata_id = ugg.user_geodata_id
  AND ugg.user_geodata_geometry_id = ugb.user_geodata_geometry_id
  AND ugb.geoid = b.geoid20
  AND b.geoid20 = g.geoid
''')

USER_GEOMETRY_SELECT_2010_BLOCKS_WITH_GEOM_BY_HASH_DIGEST = text('''
SELECT ug.name upload_name,
        ugb.geoid,
        ugg.user_geodata_geometry_id cr_geoid, 
        ugg.name, 
        ugg.original_id, 
		g.pop100,
		g.hu100,
		g.state || g.place as state_place_fips,        
        ST_asGeoJSON(ST_ForcePolygonCCW(b.geom)) geom
FROM aggregation.user_geodata ug,
    aggregation.user_geodata_geometry ugg,
    aggregation.user_geodata_blocks_2010 ugb,
    dec2010_pl94.geoheader g,
    blocks.tabblock10 b
WHERE ug.hash_digest=:hash_digest
  AND ug.user_geodata_id = ugg.user_geodata_id
  AND ugg.user_geodata_geometry_id = ugb.user_geodata_geometry_id
  AND ugb.geoid = b.geoid10
  AND b.geoid10 = g.geoid
''')

BLOCK_VINTAGE_TABLES = {
    'dec2010_pl94': 'user_geodata_blocks_2010',
    'dec2020_pl94': 'user_geodata_blocks_2020'
}

SELECT_BY_USER_GEOGRAPHY_SQL_TEMPLATE = """
SELECT ugg.user_geodata_geometry_id, 
       ugg.name, 
       ugg.original_id, 
       ST_asGeoJSON(ST_ForcePolygonCCW(ugg.geom)) geom,
       d.*
FROM aggregation.user_geodata ug,
     aggregation.user_geodata_geometry ugg,
     aggregation.{blocks_vintage_table} ugb,
     {schema}.{table_code} d
WHERE ug.hash_digest = :hash_digest
      AND ug.user_geodata_id = ugg.user_geodata_id
      AND ugg.user_geodata_geometry_id = ugb.user_geodata_geometry_id
      AND ugb.geoid = d.geoid
"""

def fetch_user_geodata(db, hash_digest):
    with db.engine.begin() as con:
        cur = con.execute(USER_GEODATA_SELECT_BY_HASH_DIGEST,hash_digest=hash_digest)
        keys = list(cur._metadata.keys)
        row = cur.first()
        if row:
            return dict(zip(keys,row))
    return None

def _fieldsFromOGRLayer(layer):
    fields = []
    ldefn = layer.GetLayerDefn()
    for n in range(ldefn.GetFieldCount()):
        fdefn = ldefn.GetFieldDefn(n)
        fields.append(fdefn.name)
    return fields


def save_user_geojson(db,
                      geojson_str,
                      hash_digest,
                      dataset_name,
                      name_field,
                      id_field,
                      source_url,
                      share_checked):
    tmp = NamedTemporaryFile('w',suffix='.json',delete=False)
    tmp.write(geojson_str)
    tmp.close()

    ogr_file = ogr.Open(tmp.name)
    if ogr_file is None:
        raise ValueError(f"ogr.Open failed for {tmp.name}")
    # assume geojson always has one layer, right?
    l = ogr_file.GetLayer(0)
    epsg = l.GetSpatialRef().GetAuthorityCode(None)
    (xmin, xmax, ymin, ymax) = l.GetExtent()
    dataset_id = None

    fields = _fieldsFromOGRLayer(l)
    with db.engine.begin() as con:
        cur = con.execute(USER_GEODATA_INSERT_SQL,
                          name=dataset_name,
                          hash_digest=hash_digest,
                          source_url=source_url,
                          public=share_checked,
                          fields=json.dumps(fields),
                          xmin=xmin,
                          ymin=ymin,
                          xmax=xmax,
                          ymax=ymax)
        dataset_id = cur.fetchall()[0][0]
        for i in range(0,l.GetFeatureCount()):
            f = l.GetFeature(i)
            mp = ogr.ForceToMultiPolygon(f.GetGeometryRef())
            properties = dict((fld, f.GetField(i)) for i,fld in enumerate(fields))
            con.execute(USER_GEODATA_GEOMETRY_INSERT_SQL,
                    user_geodata_id=dataset_id,
                    geom_wkt=mp.ExportToWkt(),
                    epsg=epsg,
                    name=properties.get(name_field),
                    original_id=properties.get(id_field),
                    properties=json.dumps(properties))

    if dataset_id is not None:
        join_user_geo_to_blocks_task.delay(dataset_id)

    return dataset_id

def list_user_geographies(db):
    cur = db.engine.execute('select *, st_asGeoJSON(bbox) bbox_json from aggregation.user_geodata where public = true order by name')
    results = []
    for row in cur:
        d = dict(row)
        bbox_json = d.pop('bbox_json')
        # parse JSON string and get rid of binary bbox
        if bbox_json:
            d['bbox'] = json.loads(bbox_json)
        else:
            del d['bbox']
        results.append(d)
    return results

def join_user_to_census(db, user_geodata_id):
    """Waffling a little on structure but this provides a single transaction-protected function which computes block joins
        for all user geographies associated with a specified user geo dataset, including clearing out anything which
        might have been there (shouldn't really be) and managing the status.
    """
    # first set the status in its own transaction so that it serves as a sign that the work is happening.
    # we may want to check the status to make sure it isn't already processing to avoid overlapping jobs
    # although the delete statements should mean that isn't a terrible problem, just a longer CPU load
    db.engine.execute(text("UPDATE aggregation.user_geodata SET status = 'PROCESSING' where user_geodata_id = :geodata_id"),geodata_id=user_geodata_id)
    with db.engine.begin() as con:
        con.execute(text("""
        DELETE FROM aggregation.user_geodata_blocks_2010 
        WHERE user_geodata_geometry_id in 
        (SELECT user_geodata_geometry_id FROM aggregation.user_geodata_geometry
         WHERE user_geodata_id=:geodata_id)"""),geodata_id=user_geodata_id)
        con.execute(text("""
        DELETE FROM aggregation.user_geodata_blocks_2020 
        WHERE user_geodata_geometry_id in 
        (SELECT user_geodata_geometry_id FROM aggregation.user_geodata_geometry
         WHERE user_geodata_id=:geodata_id)"""),geodata_id=user_geodata_id)
        con.execute(AGGREGATE_BLOCKS_2010_SQL,geodata_id=user_geodata_id)
        con.execute(AGGREGATE_BLOCKS_2020_SQL,geodata_id=user_geodata_id)
        db.engine.execute(text("UPDATE aggregation.user_geodata SET status = 'READY' where user_geodata_id = :geodata_id"),geodata_id=user_geodata_id)

def _blankFeatureCollection():
    return {
        "type": "FeatureCollection",
        "features": []
    }

def fetch_user_geog_as_geojson(db, hash_digest):
    geojson = _blankFeatureCollection()
    cur = db.engine.execute(USER_GEOMETRY_SELECT_WITH_GEOM_BY_HASH_DIGEST,hash_digest=hash_digest)
    if cur.rowcount == 0:
        raise ValueError(f"Invalid geography ID {hash_digest}")
    for cr_geoid, name, original_id, geojson_str in cur:
        base = {
            'type': 'Feature'
        }
        base['geometry'] = json.loads(geojson_str)
        base['properties'] = {
            'cr_geoid': cr_geoid
        }
        if name is not None: base['properties']['name'] = name
        if original_id is not None:
            base['properties']['original_id'] = original_id
            base['id'] = original_id
        geojson['features'].append(base)
    return geojson

USER_BLOCKS_BY_HASH_DIGEST_SQL = {
    '2020': USER_GEOMETRY_SELECT_2020_BLOCKS_WITH_GEOM_BY_HASH_DIGEST,
    '2010': USER_GEOMETRY_SELECT_2010_BLOCKS_WITH_GEOM_BY_HASH_DIGEST
}


def fetch_metadata(release=None, table_code=None):
    # for now we'll just do it from literal objects here but deepcopy them so we don't get messed up
    # maybe later we'll make a metadata schema in the database
    if table_code is None:
        raise Exception('Table code must be specified for metadata fetch')
    md = METADATA.get(table_code.lower())
    if md:
        if release is None or release in md['releases']:
            return deepcopy(md)
        if release == COMPARISON_RELEASE_CODE:
            c_10     = []
            c_20     = []
            c_change = []
            base = deepcopy(md)
            for col,label in md['columns'].items():
                c_10.append((f"{col}_2010", f"{label} (2010)"))
                c_20.append((f"{col}_2020", f"{label} (2020)"))
                c_change.append((f"{col}_pct_chg", f"{label} (% change)"))
                base['columns'] = OrderedDict(c_20 + c_10 + c_change)
            return base

    return None


def evaluateUserGeographySQLTemplate(schema, table_code):
    """Schemas and table names can't be handled as bindparams with SQLAlchemy, so
       this allows us to use a 'select *' syntax for multiple tables.
    """
    try:
        blocks_vintage_table = BLOCK_VINTAGE_TABLES[schema]
    except KeyError:
        raise ValueError(f"No blocks vintage identified for given schema {schema}")
    return SELECT_BY_USER_GEOGRAPHY_SQL_TEMPLATE.format(schema=schema, table_code=table_code, blocks_vintage_table=blocks_vintage_table)

def aggregate_decennial(db, hash_digest, release, table_code):
    """For the given user geography, identified by hash_digest, aggregate the given table
    for the given decennial census release, and return a Pandas dataframe with the results.
    In addition to the data columns for the given table, the dataframe may include columns
    'name' and/or 'original_id', if the user geography identified sources for those in their
    upload.
    """

    if fetch_metadata(release=release, table_code=table_code):
        sql = evaluateUserGeographySQLTemplate(release, table_code)
        query = text(sql).bindparams(hash_digest=hash_digest)
        logger.info(f'aggregate_decennial: starting timer {hash_digest} {release} {table_code}')
        start = timer()
        df = pd.read_sql(query, db.engine)
        end = timer()
        logger.info(f"pd.read_sql {hash_digest} {release} {table_code} elapsed time {timedelta(seconds=end-start)}")
        df = df.drop('geoid',axis=1) # we don't care about the original blocks after we groupby
        agg_funcs = dict((c,'sum') for c in df.columns[1:])
        agg_funcs['name'] = 'first'        # these string values are
        agg_funcs['original_id'] = 'first' # the same for each row aggregated
        agg_funcs['geom'] = 'first'        # by 'user_geodata_geometry_id'
        aggd = df.groupby('user_geodata_geometry_id').agg(agg_funcs)
        for c in ['name', 'original_id']:
            if aggd[c].isnull().all():
                aggd = aggd.drop(c,axis=1)
        aggd = aggd.reset_index()
        end = timer()
        logger.info(f"all processing {hash_digest} {release} {table_code} total elapsed time {timedelta(seconds=end-start)}")
        return aggd

    raise ValueError('Invalid release or table code')

def aggregate_decennial_comparison(db, hash_digest, table_code):
    agg_2020 = aggregate_decennial(db, hash_digest, 'dec2020_pl94', table_code).set_index('user_geodata_geometry_id')
    agg_2010 = aggregate_decennial(db, hash_digest, 'dec2010_pl94', table_code).set_index('user_geodata_geometry_id')
    # not all uploads have all columns, so be responsive to the data
    label_cols = []
    for c in ['name', 'original_id', 'geom']:
        if c in agg_2020:
            label_cols.append(c)
    label_df = agg_2020[label_cols]
    agg_2020 = agg_2020.drop(label_cols,axis=1)
    agg_2010 = agg_2010.drop(label_cols,axis=1)
    pct_chg = (agg_2020-agg_2010)/agg_2010
    joined = agg_2020.join(agg_2010,lsuffix='_2020',rsuffix='_2010')
    joined = joined.join(pct_chg.rename(columns=lambda x: f"{x}_change"))
    return label_df.join(joined).reset_index()

def dataframe_to_feature_collection(df: pd.DataFrame, geom_col):
    """Given a Pandas dataframe with one column stringified GeoJSON, return a
    dict representing a GeoJSON FeatureCollection, where `geom_col` is parsed and
    used for the 'geometry' and the rest of the row is converted to a 'properties' dict."""
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    for _, row in df.iterrows():
        row = row.to_dict()
        geom = row.pop(geom_col)
        f = {
            'type': 'Feature',
            'geometry': json.loads(geom),
            'properties': row
        }
        if 'original_id' in row:
            f['id'] = row['original_id']
        geojson['features'].append(f)

    return geojson

def create_block_xref_download(db, hash_digest, year):
    try:
        sql = USER_BLOCKS_BY_HASH_DIGEST_SQL[str(year)]
    except KeyError:
        raise ValueError(f"Invalid year {year}")
    df = pd.read_sql(sql.bindparams(hash_digest=hash_digest),db.engine)
    user_geo_name = str(df['upload_name'].unique().squeeze())
    df = df.drop('upload_name', axis=1)
    metadata = {
        'title': f"Census Reporter {year} Block Assignments for {user_geo_name}",
        'columns': OrderedDict((
            ('geoid', f'15-character unique block identifier'),
            ('cr_geoid', '''An arbitrary unique identifier for a specific geography (e.g. neighborhood) included in a user uploaded map'''),
            ('name', 'A name for a specific geography included in a user uploaded map, if available'),
            ('original_id', 'A unique identifier for a specific geography included in a user uploaded map, from the original source, if available'),
            ('pop100', f'The total population for the given block (Decennial Census {year})'),
            ('hu100', f'The total housing units (occupied or vacant) for the given block (Decennial Census {year})'),
            ('state_place_fips', f'The combined State/Place FIPS code for the given block (Decennial Census {year})'),
        ))
    }
    release = f'tiger{year}'
    table_code = 'block_assignments'

    tmp = write_compound_zipfile(hash_digest, release, table_code, df, metadata)
    remote_filename = build_filename(hash_digest, year, 'block_assignments', 'zip')
    move_file_to_s3(tmp.name,hash_digest,remote_filename)
    return tmp



def create_aggregate_download(db, hash_digest, release, table_code):
    if release == COMPARISON_RELEASE_CODE:
        aggregated = aggregate_decennial_comparison(db, hash_digest, table_code)
    else:
        aggregated = aggregate_decennial(db, hash_digest, release, table_code)
    metadata = fetch_metadata(release=release, table_code=table_code)

    if 'original_id' in aggregated: # original id is second if its there so insert it first
        metadata['columns']['original_id'] = 'Geographic Identifier'
        metadata['columns'].move_to_end('original_id', last=False)
    if 'name' in aggregated: # name is first if its there
        metadata['columns']['name'] = 'Geography Name'
        metadata['columns'].move_to_end('name', last=False)

    # only need it if there's no name or ID. will we even tolerate that?
    if 'name' in aggregated or 'original_id' in aggregated:
        aggregated = aggregated.drop('user_geodata_geometry_id', axis=1)
    else:
        aggregated = aggregated.rename(columns={'user_geodata_geometry_id': 'cr_geoid'})
        metadata['columns']['cr_geoid'] = 'Census Reporter Geography ID'
        metadata['columns'].move_to_end('cr_geoid', last=False)

    # NaN and inf bork JSON and inf looks bad in CSV too.
    # Any columns could have NaN, not just pct_chg -- e.g. Atlanta has n'hoods which get no 2010 blocks
    aggregated = aggregated.replace([np.inf, -np.inf, np.nan],'')

    tmp = write_compound_zipfile(hash_digest, release, table_code, aggregated, metadata)

    remote_filename = build_filename(hash_digest, release, table_code, 'zip')
    move_file_to_s3(tmp.name,hash_digest,remote_filename)
    return tmp

def write_compound_zipfile(hash_digest, release, table_code, df, metadata):
    """Given a dataframe with a 'geom' column,
    create a ZipFile with the data from that dataframe
    in both CSV and GeoJSON, returning a semi-persistent
    temporary file.
    """
    with NamedTemporaryFile('wb',suffix='.zip',delete=False) as tmp:
        with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(build_filename(hash_digest, release, table_code, 'csv'), df.drop('geom', axis=1).to_csv(index=False))
            zf.writestr(build_filename(hash_digest, release, table_code, 'geojson'), json.dumps(dataframe_to_feature_collection(df, 'geom')))
            zf.writestr(f'metadata.json', json.dumps(metadata,indent=2))
            zf.close()
    return tmp

def move_file_to_s3(local_filename, hash_digest, destination_filename):
    """Considered making this a celery task, but don't think the file created on `web` is available on `worker`
       so lets wait to see if we even need the async.
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(local_filename,
            "files.censusreporter.org",
            f"aggregation/{hash_digest}/{destination_filename}",
            ExtraArgs={'ACL': 'public-read'})

    except ClientError as e:
        logger.error(e)
        return False
    return True



def build_filename(hash_digest, release, table_code, extension):
    return f'{release}_{hash_digest}_{table_code}.{extension}'

METADATA = {
    'p1': {
        'title': 'Race',
        'releases': ['dec2010_pl94', 'dec2020_pl94'],
        'columns': OrderedDict((
            ('P0010001', 'P1-1: Total'),
            ('P0010002', 'P1-2: Population of one race'),
            ('P0010003', 'P1-3: White alone'),
            ('P0010004', 'P1-4: Black or African American alone'),
            ('P0010005', 'P1-5: American Indian and Alaska Native alone'),
            ('P0010006', 'P1-6: Asian alone'),
            ('P0010007', 'P1-7: Native Hawaiian and Other Pacific Islander alone'),
            ('P0010008', 'P1-8: Some other race alone'),
            ('P0010009', 'P1-9: Population of two or more races'),
            ('P0010010', 'P1-10: Population of two races'),
            ('P0010011', 'P1-11: White; Black or African American'),
            ('P0010012', 'P1-12: White; American Indian and Alaska Native'),
            ('P0010013', 'P1-13: White; Asian'),
            ('P0010014', 'P1-14: White; Native Hawaiian and Other Pacific Islander'),
            ('P0010015', 'P1-15: White; Some other race'),
            ('P0010016', 'P1-16: Black or African American; American Indian and Alaska Native'),
            ('P0010017', 'P1-17: Black or African American; Asian'),
            ('P0010018', 'P1-18: Black or African American; Native Hawaiian and Other Pacific Islander'),
            ('P0010019', 'P1-19: Black or African American; Some other race'),
            ('P0010020', 'P1-20: American Indian and Alaska Native; Asian'),
            ('P0010021', 'P1-21: American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0010022', 'P1-22: American Indian and Alaska Native; Some other race'),
            ('P0010023', 'P1-23: Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0010024', 'P1-24: Asian; Some other race'),
            ('P0010025', 'P1-25: Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010026', 'P1-26: Population of three races'),
            ('P0010027', 'P1-27: White; Black or African American; American Indian and Alaska Native'),
            ('P0010028', 'P1-28: White; Black or African American; Asian'),
            ('P0010029', 'P1-29: White; Black or African American; Native Hawaiian and Other Pacific Islander'),
            ('P0010030', 'P1-30: White; Black or African American; Some other race'),
            ('P0010031', 'P1-31: White; American Indian and Alaska Native; Asian'),
            ('P0010032', 'P1-32: White; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0010033', 'P1-33: White; American Indian and Alaska Native; Some other race'),
            ('P0010034', 'P1-34: White; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0010035', 'P1-35: White; Asian; Some other race'),
            ('P0010036', 'P1-36: White; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010037', 'P1-37: Black or African American; American Indian and Alaska Native; Asian'),
            ('P0010038', 'P1-38: Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0010039', 'P1-39: Black or African American; American Indian and Alaska Native; Some other race'),
            ('P0010040', 'P1-40: Black or African American; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0010041', 'P1-41: Black or African American; Asian; Some other race'),
            ('P0010042', 'P1-42: Black or African American; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010043', 'P1-43: American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0010044', 'P1-44: American Indian and Alaska Native; Asian; Some other race'),
            ('P0010045', 'P1-45: American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010046', 'P1-46: Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010047', 'P1-47: Population of four races'),
            ('P0010048', 'P1-48: White; Black or African American; American Indian and Alaska Native; Asian'),
            ('P0010049', 'P1-49: White; Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0010050', 'P1-50: White; Black or African American; American Indian and Alaska Native; Some other race'),
            ('P0010051', 'P1-51: White; Black or African American; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0010052', 'P1-52: White; Black or African American; Asian; Some other race'),
            ('P0010053', 'P1-53: White; Black or African American; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010054', 'P1-54: White; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0010055', 'P1-55: White; American Indian and Alaska Native; Asian; Some other race'),
            ('P0010056', 'P1-56: White; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010057', 'P1-57: White; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010058', 'P1-58: Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0010059', 'P1-59: Black or African American; American Indian and Alaska Native; Asian; Some other race'),
            ('P0010060', 'P1-60: Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010061', 'P1-61: Black or African American; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010062', 'P1-62: American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010063', 'P1-63: Population of five races'),
            ('P0010064', 'P1-64: White; Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0010065', 'P1-65: White; Black or African American; American Indian and Alaska Native; Asian; Some other race'),
            ('P0010066', 'P1-66: White; Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010067', 'P1-67: White; Black or African American; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010068', 'P1-68: White; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010069', 'P1-69: Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0010070', 'P1-70: Population of six races'),
            ('P0010071', 'P1-71: White; Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race')))
    },
    'p2': {
        'title': 'Hispanic or Latino, and not Hispanic or Latino by Race',
        'releases': ['dec2010_pl94', 'dec2020_pl94'],
        'columns': OrderedDict((
            ('P0020001', 'P2-1: Total'),
            ('P0020002', 'P2-2: Hispanic or Latino'),
            ('P0020003', 'P2-3: Not Hispanic or Latino'),
            ('P0020004', 'P2-4: Population of one race'),
            ('P0020005', 'P2-5: White alone'),
            ('P0020006', 'P2-6: Black or African American alone'),
            ('P0020007', 'P2-7: American Indian and Alaska Native alone'),
            ('P0020008', 'P2-8: Asian alone'),
            ('P0020009', 'P2-9: Native Hawaiian and Other Pacific Islander alone'),
            ('P0020010', 'P2-10: Some other race alone'),
            ('P0020011', 'P2-11: Population of two or more races'),
            ('P0020012', 'P2-12: Population of two races'),
            ('P0020013', 'P2-13: White; Black or African American'),
            ('P0020014', 'P2-14: White; American Indian and Alaska Native'),
            ('P0020015', 'P2-15: White; Asian'),
            ('P0020016', 'P2-16: White; Native Hawaiian and Other Pacific Islander'),
            ('P0020017', 'P2-17: White; Some other race'),
            ('P0020018', 'P2-18: Black or African American; American Indian and Alaska Native'),
            ('P0020019', 'P2-19: Black or African American; Asian'),
            ('P0020020', 'P2-20: Black or African American; Native Hawaiian and Other Pacific Islander'),
            ('P0020021', 'P2-21: Black or African American; Some other race'),
            ('P0020022', 'P2-22: American Indian and Alaska Native; Asian'),
            ('P0020023', 'P2-23: American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0020024', 'P2-24: American Indian and Alaska Native; Some other race'),
            ('P0020025', 'P2-25: Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0020026', 'P2-26: Asian; Some other race'),
            ('P0020027', 'P2-27: Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020028', 'P2-28: Population of three races'),
            ('P0020029', 'P2-29: White; Black or African American; American Indian and Alaska Native'),
            ('P0020030', 'P2-30: White; Black or African American; Asian'),
            ('P0020031', 'P2-31: White; Black or African American; Native Hawaiian and Other Pacific Islander'),
            ('P0020032', 'P2-32: White; Black or African American; Some other race'),
            ('P0020033', 'P2-33: White; American Indian and Alaska Native; Asian'),
            ('P0020034', 'P2-34: White; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0020035', 'P2-35: White; American Indian and Alaska Native; Some other race'),
            ('P0020036', 'P2-36: White; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0020037', 'P2-37: White; Asian; Some other race'),
            ('P0020038', 'P2-38: White; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020039', 'P2-39: Black or African American; American Indian and Alaska Native; Asian'),
            ('P0020040', 'P2-40: Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0020041', 'P2-41: Black or African American; American Indian and Alaska Native; Some other race'),
            ('P0020042', 'P2-42: Black or African American; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0020043', 'P2-43: Black or African American; Asian; Some other race'),
            ('P0020044', 'P2-44: Black or African American; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020045', 'P2-45: American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0020046', 'P2-46: American Indian and Alaska Native; Asian; Some other race'),
            ('P0020047', 'P2-47: American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020048', 'P2-48: Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020049', 'P2-49: Population of four races'),
            ('P0020050', 'P2-50: White; Black or African American; American Indian and Alaska Native; Asian'),
            ('P0020051', 'P2-51: White; Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0020052', 'P2-52: White; Black or African American; American Indian and Alaska Native; Some other race'),
            ('P0020053', 'P2-53: White; Black or African American; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0020054', 'P2-54: White; Black or African American; Asian; Some other race'),
            ('P0020055', 'P2-55: White; Black or African American; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020056', 'P2-56: White; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0020057', 'P2-57: White; American Indian and Alaska Native; Asian; Some other race'),
            ('P0020058', 'P2-58: White; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020059', 'P2-59: White; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020060', 'P2-60: Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0020061', 'P2-61: Black or African American; American Indian and Alaska Native; Asian; Some other race'),
            ('P0020062', 'P2-62: Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020063', 'P2-63: Black or African American; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020064', 'P2-64: American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020065', 'P2-65: Population of five races'),
            ('P0020066', 'P2-66: White; Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0020067', 'P2-67: White; Black or African American; American Indian and Alaska Native; Asian; Some other race'),
            ('P0020068', 'P2-68: White; Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020069', 'P2-69: White; Black or African American; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020070', 'P2-70: White; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020071', 'P2-71: Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0020072', 'P2-72: Population of six races'),
            ('P0020073', 'P2-73: White; Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race')))
    },
    'p3': {
        'title': 'Race for the Population 18 Years and Over',
        'releases': ['dec2010_pl94', 'dec2020_pl94'],
        'columns': OrderedDict((
            ('P0030001', 'P3-1: Total'),
            ('P0030002', 'P3-2: Population of one race'),
            ('P0030003', 'P3-3: White alone'),
            ('P0030004', 'P3-4: Black or African American alone'),
            ('P0030005', 'P3-5: American Indian and Alaska Native alone'),
            ('P0030006', 'P3-6: Asian alone'),
            ('P0030007', 'P3-7: Native Hawaiian and Other Pacific Islander alone'),
            ('P0030008', 'P3-8: Some other race alone'),
            ('P0030009', 'P3-9: Population of two or more races'),
            ('P0030010', 'P3-10: Population of two races'),
            ('P0030011', 'P3-11: White; Black or African American'),
            ('P0030012', 'P3-12: White; American Indian and Alaska Native'),
            ('P0030013', 'P3-13: White; Asian'),
            ('P0030014', 'P3-14: White; Native Hawaiian and Other Pacific Islander'),
            ('P0030015', 'P3-15: White; Some other race'),
            ('P0030016', 'P3-16: Black or African American; American Indian and Alaska Native'),
            ('P0030017', 'P3-17: Black or African American; Asian'),
            ('P0030018', 'P3-18: Black or African American; Native Hawaiian and Other Pacific Islander'),
            ('P0030019', 'P3-19: Black or African American; Some other race'),
            ('P0030020', 'P3-20: American Indian and Alaska Native; Asian'),
            ('P0030021', 'P3-21: American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0030022', 'P3-22: American Indian and Alaska Native; Some other race'),
            ('P0030023', 'P3-23: Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0030024', 'P3-24: Asian; Some other race'),
            ('P0030025', 'P3-25: Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030026', 'P3-26: Population of three races'),
            ('P0030027', 'P3-27: White; Black or African American; American Indian and Alaska Native'),
            ('P0030028', 'P3-28: White; Black or African American; Asian'),
            ('P0030029', 'P3-29: White; Black or African American; Native Hawaiian and Other Pacific Islander'),
            ('P0030030', 'P3-30: White; Black or African American; Some other race'),
            ('P0030031', 'P3-31: White; American Indian and Alaska Native; Asian'),
            ('P0030032', 'P3-32: White; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0030033', 'P3-33: White; American Indian and Alaska Native; Some other race'),
            ('P0030034', 'P3-34: White; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0030035', 'P3-35: White; Asian; Some other race'),
            ('P0030036', 'P3-36: White; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030037', 'P3-37: Black or African American; American Indian and Alaska Native; Asian'),
            ('P0030038', 'P3-38: Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0030039', 'P3-39: Black or African American; American Indian and Alaska Native; Some other race'),
            ('P0030040', 'P3-40: Black or African American; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0030041', 'P3-41: Black or African American; Asian; Some other race'),
            ('P0030042', 'P3-42: Black or African American; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030043', 'P3-43: American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0030044', 'P3-44: American Indian and Alaska Native; Asian; Some other race'),
            ('P0030045', 'P3-45: American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030046', 'P3-46: Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030047', 'P3-47: Population of four races'),
            ('P0030048', 'P3-48: White; Black or African American; American Indian and Alaska Native; Asian'),
            ('P0030049', 'P3-49: White; Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0030050', 'P3-50: White; Black or African American; American Indian and Alaska Native; Some other race'),
            ('P0030051', 'P3-51: White; Black or African American; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0030052', 'P3-52: White; Black or African American; Asian; Some other race'),
            ('P0030053', 'P3-53: White; Black or African American; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030054', 'P3-54: White; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0030055', 'P3-55: White; American Indian and Alaska Native; Asian; Some other race'),
            ('P0030056', 'P3-56: White; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030057', 'P3-57: White; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030058', 'P3-58: Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0030059', 'P3-59: Black or African American; American Indian and Alaska Native; Asian; Some other race'),
            ('P0030060', 'P3-60: Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030061', 'P3-61: Black or African American; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030062', 'P3-62: American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030063', 'P3-63: Population of five races'),
            ('P0030064', 'P3-64: White; Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0030065', 'P3-65: White; Black or African American; American Indian and Alaska Native; Asian; Some other race'),
            ('P0030066', 'P3-66: White; Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030067', 'P3-67: White; Black or African American; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030068', 'P3-68: White; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030069', 'P3-69: Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0030070', 'P3-70: Population of six races'),
            ('P0030071', 'P3-71: White; Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race')))
    },
    'p4': {
        'title': 'Hispanic or Latino, and not Hispanic or Latino by Race for the Population 18 Years and Over',
        'releases': ['dec2010_pl94', 'dec2020_pl94'],
        'columns': OrderedDict((
            ('P0040001', 'P4-1: Total'),
            ('P0040002', 'P4-2: Hispanic or Latino'),
            ('P0040003', 'P4-3: Not Hispanic or Latino'),
            ('P0040004', 'P4-4: Population of one race'),
            ('P0040005', 'P4-5: White alone'),
            ('P0040006', 'P4-6: Black or African American alone'),
            ('P0040007', 'P4-7: American Indian and Alaska Native alone'),
            ('P0040008', 'P4-8: Asian alone'),
            ('P0040009', 'P4-9: Native Hawaiian and Other Pacific Islander alone'),
            ('P0040010', 'P4-10: Some other race alone'),
            ('P0040011', 'P4-11: Population of two or more races'),
            ('P0040012', 'P4-12: Population of two races'),
            ('P0040013', 'P4-13: White; Black or African American'),
            ('P0040014', 'P4-14: White; American Indian and Alaska Native'),
            ('P0040015', 'P4-15: White; Asian'),
            ('P0040016', 'P4-16: White; Native Hawaiian and Other Pacific Islander'),
            ('P0040017', 'P4-17: White; Some other race'),
            ('P0040018', 'P4-18: Black or African American; American Indian and Alaska Native'),
            ('P0040019', 'P4-19: Black or African American; Asian'),
            ('P0040020', 'P4-20: Black or African American; Native Hawaiian and Other Pacific Islander'),
            ('P0040021', 'P4-21: Black or African American; Some other race'),
            ('P0040022', 'P4-22: American Indian and Alaska Native; Asian'),
            ('P0040023', 'P4-23: American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0040024', 'P4-24: American Indian and Alaska Native; Some other race'),
            ('P0040025', 'P4-25: Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0040026', 'P4-26: Asian; Some other race'),
            ('P0040027', 'P4-27: Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040028', 'P4-28: Population of three races'),
            ('P0040029', 'P4-29: White; Black or African American; American Indian and Alaska Native'),
            ('P0040030', 'P4-30: White; Black or African American; Asian'),
            ('P0040031', 'P4-31: White; Black or African American; Native Hawaiian and Other Pacific Islander'),
            ('P0040032', 'P4-32: White; Black or African American; Some other race'),
            ('P0040033', 'P4-33: White; American Indian and Alaska Native; Asian'),
            ('P0040034', 'P4-34: White; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0040035', 'P4-35: White; American Indian and Alaska Native; Some other race'),
            ('P0040036', 'P4-36: White; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0040037', 'P4-37: White; Asian; Some other race'),
            ('P0040038', 'P4-38: White; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040039', 'P4-39: Black or African American; American Indian and Alaska Native; Asian'),
            ('P0040040', 'P4-40: Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0040041', 'P4-41: Black or African American; American Indian and Alaska Native; Some other race'),
            ('P0040042', 'P4-42: Black or African American; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0040043', 'P4-43: Black or African American; Asian; Some other race'),
            ('P0040044', 'P4-44: Black or African American; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040045', 'P4-45: American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0040046', 'P4-46: American Indian and Alaska Native; Asian; Some other race'),
            ('P0040047', 'P4-47: American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040048', 'P4-48: Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040049', 'P4-49: Population of four races'),
            ('P0040050', 'P4-50: White; Black or African American; American Indian and Alaska Native; Asian'),
            ('P0040051', 'P4-51: White; Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander'),
            ('P0040052', 'P4-52: White; Black or African American; American Indian and Alaska Native; Some other race'),
            ('P0040053', 'P4-53: White; Black or African American; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0040054', 'P4-54: White; Black or African American; Asian; Some other race'),
            ('P0040055', 'P4-55: White; Black or African American; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040056', 'P4-56: White; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0040057', 'P4-57: White; American Indian and Alaska Native; Asian; Some other race'),
            ('P0040058', 'P4-58: White; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040059', 'P4-59: White; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040060', 'P4-60: Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0040061', 'P4-61: Black or African American; American Indian and Alaska Native; Asian; Some other race'),
            ('P0040062', 'P4-62: Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040063', 'P4-63: Black or African American; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040064', 'P4-64: American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040065', 'P4-65: Population of five races'),
            ('P0040066', 'P4-66: White; Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander'),
            ('P0040067', 'P4-67: White; Black or African American; American Indian and Alaska Native; Asian; Some other race'),
            ('P0040068', 'P4-68: White; Black or African American; American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040069', 'P4-69: White; Black or African American; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040070', 'P4-70: White; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040071', 'P4-71: Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'),
            ('P0040072', 'P4-72: Population of six races'),
            ('P0040073', 'P4-73: White; Black or African American; American Indian and Alaska Native; Asian; Native Hawaiian and Other Pacific Islander; Some other race'))),
    },
    'p5': {
        'title': 'Group Quarters Population by Major Group Quarters Type',
        'releases': ['dec2020_pl94'],
        'columns': OrderedDict((
            ('P0050001', 'Total:'),
            ('P0050002', 'Institutionalized population:'),
            ('P0050003', 'Correctional facilities for adults'),
            ('P0050004', 'Juvenile facilities'),
            ('P0050005', 'Nursing facilities/Skilled-nursing facilities'),
            ('P0050006', 'Other institutional facilities'),
            ('P0050007', 'Noninstitutionalized population:'),
            ('P0050008', 'College/University student housing'),
            ('P0050009', 'Military quarters'),
            ('P0050010', 'Other noninstitutional facilities'),
        ))
    },
    'h1': {
        'title': 'Occupancy Status',
        'releases': ['dec2010_pl94', 'dec2020_pl94'],
        'columns': OrderedDict((
            ('H0010001', 'H1-1: Total'),
            ('H0010002', 'H1-2: Occupied'),
            ('H0010003', 'H1-3: Vacant'))),
    }
}
