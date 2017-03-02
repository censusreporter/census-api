import urlparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from openpyxl.styles import Alignment

Session = sessionmaker()

_sessions = {}
def session(sql_url):
    try:
        return _sessions[sql_url]
    except KeyError: # probably not super thread-safe, but repeated execution should be harmless
        engine = create_engine(sql_url)
        _sessions[sql_url] = Session(bind=engine.connect())
        return _sessions[sql_url]

def get_sql_config(sql_url):
    """Return a tuple of strings: (host, user, password, database)"""
    db_details = urlparse.urlparse(sql_url)
    return (db_details.hostname,
            db_details.username,
            db_details.password,
            db_details.path[1:])

def create_excel_download(sql_url, data, table_metadata, valid_geo_ids, file_ident, out_filename, format):
    import openpyxl
    wb = openpyxl.workbook.Workbook()
    sheet_name = ', '.join(table_metadata)
    sheet = wb.active
    sheet.title = sheet_name + ' Absolute'

    sheet['A1'] = sheet_name

    header = []
    for i, (table_id, table) in enumerate(table_metadata.iteritems()):
        # Write table titles on the first row
        # i + 2 to account for 1-indexed and sheet name in A1
        sheet.cell(row=1, column=i + 2).value = table['title']
        # Column headers
        for column_id, column_info in table['columns'].iteritems():
            column_name_utf8 = column_info['name'].encode('utf-8')
            header.append(column_name_utf8)

    sheet['A2'] = 'geoid'
    sheet['B2'] = 'name'
    sheet.merge_cells('A2:A3')
    sheet.merge_cells('B2:B3')

    for i, h in enumerate(header):
        current_col = i * 2 + 3 # 1-based index, 'geoid' and 'name' already populate first two cols
        current_cell = sheet.cell(row=2, column=current_col)
        current_cell.value = h
        current_cell.alignment = Alignment(horizontal='center')
        sheet.merge_cells(start_row=2, start_column=current_col, end_row=2, end_column=current_col + 1)

    for i in range(len(header) * 2):
        if i % 2 != 0:
            # 1-based index, 'geoid' and 'name' already populate first two cols
            sheet.cell(row=3, column=i + 3).value = 'Value'
        if i % 2 == 0:
            # 1-based index, 'geoid' and 'name' already populate first two cols
            sheet.cell(row=3, column=i + 3).value = 'Error'

    # this SQL echoed in OGR export but no geom so copying instead of factoring out
    # plus different binding when using SQLAlchemy
    result = session(sql_url).execute(
        """SELECT full_geoid,display_name
                 FROM tiger2014.census_name_lookup
                 WHERE full_geoid IN :geoids
                 ORDER BY full_geoid""",
        {'geoids': tuple(valid_geo_ids)}
    )
    for i, (geoid, name) in enumerate(result):
        row_num = i + 4 # one-indexed, and there's three lines of header
        row_data = [geoid, name]
        for (table_id, table) in table_metadata.iteritems():
            table_estimates = data[geoid][table_id]['estimate']
            table_errors = data[geoid][table_id]['error']
            for column_id, column_info in table['columns'].iteritems():
                row_data.append(table_estimates[column_id])
                row_data.append(table_errors[column_id])
        for j, value in enumerate(row_data):
            sheet.cell(row=row_num,column=j+1).value = value

    sheet_percents = wb.create_sheet(sheet_name + ' Percents')

    wb.save(out_filename)

def create_ogr_download(sql_url, data, table_metadata, valid_geo_ids, file_ident, out_filename, format):
    import ogr
    import osr
    format_info = supported_formats[format]
    driver_name = format_info['driver']
    ogr.UseExceptions()
    in_driver = ogr.GetDriverByName("PostgreSQL")
    host, user, password, database = get_sql_config(sql_url)
    conn = in_driver.Open("PG: host=%s dbname=%s user=%s password=%s" % (host, database, user, password))

    if conn is None:
        raise Exception("Could not connect to database to generate download.")

    out_driver = ogr.GetDriverByName(driver_name)
    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(4326)
    out_data = out_driver.CreateDataSource(out_filename)
    # See http://gis.stackexchange.com/questions/53920/ogr-createlayer-returns-typeerror
    out_layer = out_data.CreateLayer(file_ident.encode('utf-8'), srs=out_srs, geom_type=ogr.wkbMultiPolygon)
    out_layer.CreateField(ogr.FieldDefn('geoid', ogr.OFTString))
    out_layer.CreateField(ogr.FieldDefn('name', ogr.OFTString))
    for (table_id, table) in table_metadata.iteritems():
        for column_id, column_info in table['columns'].iteritems():
            column_name_utf8 = column_id.encode('utf-8')
            if driver_name == "ESRI Shapefile":
                # Work around the Shapefile column name length limits
                out_layer.CreateField(ogr.FieldDefn(column_name_utf8, ogr.OFTReal))
                out_layer.CreateField(ogr.FieldDefn(column_name_utf8 + "e", ogr.OFTReal))
            else:
                out_layer.CreateField(ogr.FieldDefn(column_name_utf8, ogr.OFTReal))
                out_layer.CreateField(ogr.FieldDefn(column_name_utf8 + ", Error", ogr.OFTReal))

    # this SQL echoed in Excel export but no geom so copying instead of factoring out
    sql = """SELECT geom,full_geoid,display_name
             FROM tiger2014.census_name_lookup
             WHERE full_geoid IN (%s)
             ORDER BY full_geoid""" % ', '.join("'%s'" % g.encode('utf-8') for g in valid_geo_ids)
    in_layer = conn.ExecuteSQL(sql)

    in_feat = in_layer.GetNextFeature()
    while in_feat is not None:
        out_feat = ogr.Feature(out_layer.GetLayerDefn())
        if format in ('shp', 'kml', 'geojson'):
            out_feat.SetGeometry(in_feat.GetGeometryRef())
        geoid = in_feat.GetField('full_geoid')
        out_feat.SetField('geoid', geoid)
        out_feat.SetField('name', in_feat.GetField('display_name'))
        for (table_id, table) in table_metadata.iteritems():
            table_estimates = data[geoid][table_id]['estimate']
            table_errors = data[geoid][table_id]['error']
            for column_id, column_info in table['columns'].iteritems():
                column_name_utf8 = column_id.encode('utf-8')
                if column_id in table_estimates:
                    if format == 'shp':
                        # Work around the Shapefile column name length limits
                        estimate_col_name = column_name_utf8
                        error_col_name = column_name_utf8 + "e"
                    else:
                        estimate_col_name = column_name_utf8
                        error_col_name = column_name_utf8 + ", Error"

                    out_feat.SetField(estimate_col_name, table_estimates[column_id])
                    out_feat.SetField(error_col_name, table_errors[column_id])

        out_layer.CreateFeature(out_feat)
        in_feat.Destroy()
        in_feat = in_layer.GetNextFeature()
    out_data.Destroy()

supported_formats = { # these should all have a 'function' with the right signature
    'shp':      {"function": create_ogr_download, "driver": "ESRI Shapefile"},
    'kml':      {"function": create_ogr_download, "driver": "KML"},
    'geojson':  {"function": create_ogr_download, "driver": "GeoJSON"},
    'xlsx':     {"function": create_excel_download, "driver": "XLSX"},
    'csv':      {"function": create_ogr_download, "driver": "CSV"},
}
