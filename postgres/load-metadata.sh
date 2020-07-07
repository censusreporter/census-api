#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE TABLE public.census_tabulation_metadata (
      tabulation_code varchar(6),
      table_title text,
      simple_table_title text,
      subject_area text,
      universe text,
      topics text[],
      weight smallint,
      tables_in_one_yr text[],
      tables_in_three_yr text[],
      tables_in_five_yr text[],
      PRIMARY KEY (tabulation_code)
    )
    WITH (autovacuum_enabled = FALSE);
    CREATE INDEX ON public.census_tabulation_metadata USING GIN(topics);
    CREATE INDEX ON public.census_tabulation_metadata (lower(table_title) text_pattern_ops);

  COPY public.census_tabulation_metadata
    FROM '/docker-entrypoint-initdb.d/unified_metadata.csv'
    WITH csv ENCODING 'utf8' HEADER;
EOSQL
