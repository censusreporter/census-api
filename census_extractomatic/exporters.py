import urlparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import openpyxl
from openpyxl.styles import Alignment, Font

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
    def excel_helper(sheet, table_id, table, option):
        """
        Create excel sheet.

        :param option: 'value' or 'percent'
        """

        # Write table id and title on the first row
        sheet['A1'] = table_id
        sheet['B1'] = table['title']

        sheet['A1'].font = Font(bold=True)
        sheet['B1'].font = Font(bold=True)

        header = []
        max_indent = 0
        # get column headers
        for column_id, column_info in table['columns'].iteritems():
            column_name_utf8 = column_info['name'].encode('utf-8')
            indent = column_info['indent']

            header.append((column_name_utf8, indent))

            if indent > max_indent:
                max_indent = indent

        # Populate first column with headers
        for i, col_tuple in enumerate(header):
            current_row = i + 4 # 1-based index, account for geographic headers
            current_cell = sheet.cell(row=current_row, column=1)
            current_cell.value = col_tuple[0]
            current_cell.alignment = Alignment(indent=col_tuple[1], wrap_text=True)

        # Resize column width
        sheet.column_dimensions['A'].width = 50

        # this SQL echoed in OGR export but no geom so copying instead of factoring out
        # plus different binding when using SQLAlchemy
        result = session(sql_url).execute(
            """SELECT full_geoid,display_name
                     FROM tiger2014.census_name_lookup
                     WHERE full_geoid IN :geoids
                     ORDER BY full_geoid""",
            {'geoids': tuple(valid_geo_ids)}
        )

        geo_headers = []
        for i, (geoid, name) in enumerate(result):
            geo_headers.append(name)
            col_values = []
            col_errors = []
            for (table_id, table) in table_metadata.iteritems():
                table_estimates = data[geoid][table_id]['estimate']
                table_errors = data[geoid][table_id]['error']
                if option == 'value':
                    for column_id, column_info in table['columns'].iteritems():
                        col_values.append(table_estimates[column_id])
                        col_errors.append(table_errors[column_id])
                elif option == 'percent':
                    base_estimate = data[geoid][table_id]['estimate'][table['denominator_column_id']]
                    for column_id, column_info in table['columns'].iteritems():
                        col_values.append(table_estimates[column_id] / base_estimate)
                        col_errors.append(table_errors[column_id] / base_estimate)
            for j, value in enumerate(col_values):
                col_num = (i + 1) * 2
                row_num = j + 4
                sheet.cell(row=row_num, column=col_num).value = value
                sheet.cell(row=row_num, column=col_num + 1).value = col_errors[j]
                if option == 'percent':
                    sheet.cell(row=row_num, column=col_num).number_format = '0.00%'
                    sheet.cell(row=row_num, column=col_num + 1).number_format = '0.00%'

        # Write geo headers
        for i in range(len(geo_headers)):
            current_col = (i + 1) * 2
            current_cell = sheet.cell(row=2, column=current_col)
            current_cell.value = geo_headers[i]
            current_cell.alignment = Alignment(horizontal='center')
            sheet.merge_cells(start_row=2, end_row=2, start_column=current_col, end_column=current_col + 1)
            sheet.cell(row=3, column=current_col).value = "Value"
            sheet.cell(row=3, column=current_col + 1).value = "Error"

        # sheet['A2'] = 'geoid'
        # sheet['B2'] = 'name'
        #
        # header = []
        # max_indent = 0
        # # Column headers
        # for column_id, column_info in table['columns'].iteritems():
        #     column_name_utf8 = column_info['name'].encode('utf-8')
        #     indent = column_info['indent']
        #
        #     header.append((column_name_utf8, indent))
        #
        #     if indent > max_indent:
        #         max_indent = indent
        #
        # for i, col_tuple in enumerate(header):
        #     current_col = i * 2 + 3 # 1-based index, 'geoid' and 'name' already populate first two cols
        #     current_row = 2 + col_tuple[1]
        #     current_cell = sheet.cell(row=current_row, column=current_col)
        #     current_cell.value = col_tuple[0]
        #     current_cell.alignment = Alignment(horizontal='center')
        #     sheet.merge_cells(start_row=current_row, start_column=current_col, end_row=2 + max_indent, end_column=current_col + 1)
        #
        # sheet.merge_cells('A2:A' + str(2 + max_indent))
        # sheet.merge_cells('B2:B' + str(2 + max_indent))
        #
        # for i in range(len(header) * 2):
        #     if i % 2 == 0:
        #         # 1-based index, 'geoid' and 'name' already populate first two cols
        #         sheet.cell(row=3 + max_indent, column=i + 3).value = 'Value'
        #     if i % 2 != 0:
        #         # 1-based index, 'geoid' and 'name' already populate first two cols
        #         sheet.cell(row=3 + max_indent, column=i + 3).value = 'Error'
        #
        # # this SQL echoed in OGR export but no geom so copying instead of factoring out
        # # plus different binding when using SQLAlchemy
        # result = session(sql_url).execute(
        #     """SELECT full_geoid,display_name
        #              FROM tiger2014.census_name_lookup
        #              WHERE full_geoid IN :geoids
        #              ORDER BY full_geoid""",
        #     {'geoids': tuple(valid_geo_ids)}
        # )
        # for i, (geoid, name) in enumerate(result):
        #     row_num = i + 3 + max_indent # one-indexed, and account for header
        #     row_data = [geoid, name]
        #     for (table_id, table) in table_metadata.iteritems():
        #         table_estimates = data[geoid][table_id]['estimate']
        #         table_errors = data[geoid][table_id]['error']
        #         if option == 'value':
        #             for column_id, column_info in table['columns'].iteritems():
        #                 row_data.append(table_estimates[column_id])
        #                 row_data.append(table_errors[column_id])
        #         elif option == 'percent':
        #             base_estimate = data[geoid][table_id]['estimate'][table['denominator_column_id']]
        #             for column_id, column_info in table['columns'].iteritems():
        #                 row_data.append(table_estimates[column_id] / base_estimate)
        #                 row_data.append(table_errors[column_id] / base_estimate)
        #     for j, value in enumerate(row_data):
        #         sheet.cell(row=row_num,column=j+1).value = value
        #         if option == 'percent':
        #             sheet.cell(row=row_num,column=j+1).number_format = '0.00%'


    wb = openpyxl.workbook.Workbook()

    # For every table in table_metadata, make a two sheets (values and percentages)
    for i, (table_id, table) in enumerate(table_metadata.iteritems()):
        sheet = wb.active
        sheet.title = table_id + ' Values'
        excel_helper(sheet, table_id, table, 'value')

        sheet_percents = wb.create_sheet(table_id + ' Percentages')
        excel_helper(sheet_percents, table_id, table, 'percent')

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
