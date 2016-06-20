import psycopg2

conn = psycopg2.connect("dbname=census user=census")
cur = conn.cursor()

cur.execute("SELECT sumlevel, display_name, full_geoid from	tiger2014.census_name_lookup limit 10;")
results = cur.fetchall()
print results