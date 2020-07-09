FROM postgis/postgis:10-2.5

ADD https://raw.githubusercontent.com/censusreporter/census-table-metadata/master/precomputed/unified_metadata.csv /docker-entrypoint-initdb.d/
RUN chmod 644 /docker-entrypoint-initdb.d/unified_metadata.csv
COPY load-metadata.sh /docker-entrypoint-initdb.d/
COPY data/*.sql.gz /docker-entrypoint-initdb.d/
