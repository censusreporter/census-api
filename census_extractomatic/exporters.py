import urlparse

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
    sheet.title = sheet_name
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
