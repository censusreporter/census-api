import ogr
import osr
import urlparse
import os
class OGRExporter(object):
    """docstring for OGRExporter"""
    def __init__(self, inner_path, driver_name, file_ident, format, table_metadata):
        super(OGRExporter,self).__init__()
        self.format = format

        out_filename = os.path.join(inner_path, '%s.%s' % (file_ident, format))
        out_driver = ogr.GetDriverByName(driver_name)
        out_srs = osr.SpatialReference()
        out_srs.ImportFromEPSG(4326)
        self.out_data = out_driver.CreateDataSource(out_filename)
        # See http://gis.stackexchange.com/questions/53920/ogr-createlayer-returns-typeerror
        out_layer = self.out_data.CreateLayer(file_ident.encode('utf-8'), srs=out_srs, geom_type=ogr.wkbMultiPolygon)
        out_layer.CreateField(ogr.FieldDefn('geoid', ogr.OFTString))
        out_layer.CreateField(ogr.FieldDefn('name', ogr.OFTString))

        for (table_id, table) in table_metadata.iteritems():
            for column_id, column_info in table['columns'].iteritems():
                estimate_col_name, error_col_name = self.make_column_names(column_id, column_info)                
                out_layer.CreateField(ogr.FieldDefn(estimate_col_name, ogr.OFTReal))
                out_layer.CreateField(ogr.FieldDefn(error_col_name, ogr.OFTReal))

        self.out_layer = out_layer

    def destroy(self):
        self.out_data.Destroy()

    def make_column_names(self, column_id, column_info):
        if format == 'shp':
            # Work around the Shapefile column name length limits
            estimate_col_name = column_id
            error_col_name = column_id+"e"
        else:
            estimate_col_name = column_id + " - " +column_info['name']
            error_col_name = column_id + " - " +column_info['name']+", Error"

        return (estimate_col_name, error_col_name)    

    def export_feature(self, in_feat):
        out_feat = ogr.Feature(self.out_layer.GetLayerDefn())
        out_feat.SetGeometry(in_feat.GetGeometryRef())
        geoid = in_feat.GetField('full_geoid')
        out_feat.SetField('geoid', geoid)
        out_feat.SetField('name', in_feat.GetField('display_name'))
        for (table_id, table) in table_metadata.iteritems():
            table_estimates = data[geoid][table_id]['estimate']
            table_errors = data[geoid][table_id]['error']
            for column_id, column_info in table['columns'].iteritems():
                if column_id in table_estimates:
                    estimate_col_name, error_col_name = self.make_column_names(column_id, column_info)
                    out_feat.SetField(estimate_col_name, table_estimates[column_id])
                    out_feat.SetField(error_col_name, table_errors[column_id])

        self.out_layer.CreateFeature(out_feat)

def ogr_export(database_uri, driver, file_ident, table_metadata, cursor, format, valid_geo_ids, inner_path):
    """Gnarly list of arguments, but it let us take a bunch of lines out of api.py"""

    ogr.UseExceptions()
    db_details = urlparse.urlparse(database_uri)
    host = db_details.hostname
    user = db_details.username
    password = db_details.password
    database = db_details.path[1:]
    in_driver = ogr.GetDriverByName("PostgreSQL")
    conn = in_driver.Open("PG: host=%s dbname=%s user=%s password=%s" % (host, database, user, password))

    if conn is None:
        raise Exception("Could not connect to database to generate download.")

    exporter = OGRExporter(inner_path, driver, file_ident, format, table_metadata)
    sql = cursor.mogrify("""SELECT the_geom,full_geoid,display_name
        FROM tiger2012.census_name_lookup
        WHERE full_geoid IN %s
        ORDER BY full_geoid""", [tuple(valid_geo_ids)])
    in_layer = conn.ExecuteSQL(sql)

    in_feat = in_layer.GetNextFeature()
    while in_feat is not None:
        exporter.export_feature(in_feat)
        in_feat.Destroy()
        in_feat = in_layer.GetNextFeature()

    exporter.destroy()