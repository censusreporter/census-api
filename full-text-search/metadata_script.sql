-- Creates search_metadata table with columns
-- -- text1: display_name or tabulation_code,
-- -- text2: sumlevel or table_title,
-- -- text3: sumlevel_name or topics,
-- -- text4: full_geoid or simple_table_title,
-- -- text5: population or tables,
-- -- text6: priority or NULL
-- -- type: 'profile' or 'table',
-- -- document
-- by pulling information about profiles (subquery before the UNION)
-- and about tables (subquery after the UNION). This creates just one metadata
-- table with all of the information we (currently) need for search.

DROP TABLE IF EXISTS search_metadata;

CREATE TABLE search_metadata AS (
    SELECT CAST(display_name as text) AS text1,
           CAST(sumlevel as text) AS text2,
           NULL AS text3,
           CAST(full_geoid as text) as text4,
           CAST(population as text) as text5,
           CAST(priority as text) as text6,
           'profile' AS type,
           document AS document -- add conditional and document || to tsvector
    FROM (
        SELECT display_name, sumlevel, full_geoid, population, priority,
               setweight(to_tsvector('simple', coalesce(display_name, ' ')), 'A') ||
               setweight(to_tsvector('simple', coalesce(full_geoid, ' ')), 'A') AS document
        -- Exclude sumlevels without maps (067, 258, 355)
        FROM (
            SELECT DISTINCT display_name, sumlevel, full_geoid,
                            population, priority
            FROM tiger2017.census_name_lookup
            WHERE sumlevel NOT IN ('067', '258', '355')
            ) profile_search
        ) profile_documents

    -- Explanation of above query:
    --
    -- Information about each place (name, sumlevel, etc.) is pulled
    -- directly from tiger data, and the name is transformed into a tsvector
    -- for a full text search. For full detail, refer to the psql docs.
    --
    -- The column text3 is NULL initially, but it will be populated with
    -- names of sumlevels. (See the update statements below this.)
    --
    -- From this, we take the columns directly used in search results
    -- and put them into the combined metadata table.

    UNION

    -- Explanation of below query:
    --
    -- Innermost query joins the table with all table names
    -- (census_table_metadata) to the table with column titles
    -- (census_column_metadata) based on table ID.
    --
    -- It selects all of the column titles, along with other table
    -- info, and returns a table with one row for every column
    -- in every table plus the metadata (table_id, universe,
    -- etc.) as other entries in each row.
    --
    -- The result of this is called table_search.
    --
    -- The query outside that transforms relevant information into a document,
    -- i.e., a tsvector, by using the table title and other data.
    -- coalesce is used because the columns have the potential to
    -- have null values. string_agg creates one string of all
    -- column names. For full detail, refer to the psql docs.
    --
    -- This creates a table with one row for every table in the
    -- acs2017_1yr schema, with columns table_id, table_title, etc.,
    -- and document (the tsvector, the most important for search).
    --
    -- From this, we take the columns directly used in search results
    -- and put them into the combined metadata table.

    SELECT CAST(tabulation_code as text) AS text1,
           CAST(table_title as text) AS text2,
           CAST(array_to_string(topics, ', ') as text) AS text3,
           CAST(simple_table_title as text) AS text4,
           CAST(array_to_string(tables, ' ') as text) AS text5,
           NULL AS text6,
           'table' AS type,
           document as document
    FROM (
        SELECT tabulation_code, table_title, topics, simple_table_title,
               tables_in_one_yr as tables,
               setweight(to_tsvector(coalesce(table_title, ' ')), 'A') ||
               setweight(to_tsvector(coalesce(tabulation_code)), 'A') ||
               setweight(to_tsvector(coalesce(array_to_string(tables_in_one_yr, ' '), ' ')), 'A') ||
               setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') ||
               setweight(to_tsvector(coalesce(string_agg(column_title, ' '), ' ')), 'C') ||
               setweight(to_tsvector(coalesce(universe, ' ')), 'D') as document
        FROM (
            SELECT DISTINCT t.tabulation_code,
                            t.table_title,
                            t.simple_table_title,
                            t.tables_in_one_yr,
                            t.topics,
                            t.subject_area,
                            t.universe,
                            c.column_title
            FROM census_tabulation_metadata t
            JOIN acs2017_1yr.census_column_metadata c
            ON t.tables_in_one_yr[1] = c.table_id
            ) table_search
        WHERE tabulation_code = table_search.tabulation_code
        GROUP BY tabulation_code, table_title, tables_in_one_yr, topics,
                 subject_area, universe, simple_table_title
        ) table_documents
    );

-- Because there isn't a good way to set sumlevel names (they aren't stored
-- in a table anywhere), we use a host of update statements to set them all.
-- The level/name pairings were taken from census_extractomatic/api.py.

UPDATE search_metadata SET text3 = 'nation' WHERE text2 = '010' AND type = 'profile';
UPDATE search_metadata SET text3 = 'region' WHERE text2 = '020' AND type = 'profile';
UPDATE search_metadata SET text3 = 'division' WHERE text2 = '030' AND type = 'profile';
UPDATE search_metadata SET text3 = 'state' WHERE text2 = '040' AND type = 'profile';
UPDATE search_metadata SET text3 = 'county' WHERE text2 = '050' AND type = 'profile';
UPDATE search_metadata SET text3 = 'county subdivision' WHERE text2 = '060' AND type = 'profile';
UPDATE search_metadata SET text3 = 'block' WHERE text2 = '101' AND type = 'profile';
UPDATE search_metadata SET text3 = 'census tract' WHERE text2 = '140' AND type = 'profile';
UPDATE search_metadata SET text3 = 'block group' WHERE text2 = '150' AND type = 'profile';
UPDATE search_metadata SET text3 = 'place' WHERE text2 = '160' AND type = 'profile';
UPDATE search_metadata SET text3 = 'consolidated city' WHERE text2 = '170' AND type = 'profile';
UPDATE search_metadata SET text3 = 'Alaska native regional corporation' WHERE text2 = '230' AND type = 'profile';
UPDATE search_metadata SET text3 = 'native area' WHERE text2 = '250' AND type = 'profile';
UPDATE search_metadata SET text3 = 'tribal subdivision' WHERE text2 = '251' AND type = 'profile';
UPDATE search_metadata SET text3 = 'native area (reservation)' WHERE text2 = '252' AND type = 'profile';
UPDATE search_metadata SET text3 = 'native area (off-reservation trust land)/Hawaiian Homeland' WHERE text2 = '254' AND type = 'profile';
UPDATE search_metadata SET text3 = 'tribal census tract' WHERE text2 = '256' AND type = 'profile';
UPDATE search_metadata SET text3 = 'MSA' WHERE text2 = '300' AND type = 'profile';
UPDATE search_metadata SET text3 = 'CBSA' WHERE text2 = '310' AND type = 'profile';
UPDATE search_metadata SET text3 = 'metropolitan division' WHERE text2 = '314' AND type = 'profile';
UPDATE search_metadata SET text3 = 'CSA' WHERE text2 = '330' AND type = 'profile';
UPDATE search_metadata SET text3 = 'combined NECTA' WHERE text2 = '335' AND type = 'profile';
UPDATE search_metadata SET text3 = 'NECTA' WHERE text2 = '350' AND type = 'profile';
UPDATE search_metadata SET text3 = 'NECTA division' WHERE text2 = '364' AND type = 'profile';
UPDATE search_metadata SET text3 = 'urban area' WHERE text2 = '400' AND type = 'profile';
UPDATE search_metadata SET text3 = 'congressional district' WHERE text2 = '500' AND type = 'profile';
UPDATE search_metadata SET text3 = 'state senate district' WHERE text2 = '610' AND type = 'profile';
UPDATE search_metadata SET text3 = 'state house district' WHERE text2 = '620' AND type = 'profile';
UPDATE search_metadata SET text3 = 'PUMA' WHERE text2 = '795' AND type = 'profile';
UPDATE search_metadata SET text3 = 'ZCTA3' WHERE text2 = '850' AND type = 'profile';
UPDATE search_metadata SET text3 = 'ZCTA5' WHERE text2 = '860' AND type = 'profile';
UPDATE search_metadata SET text3 = 'elementary school district' WHERE text2 = '950' AND type = 'profile';
UPDATE search_metadata SET text3 = 'secondary school district' WHERE text2 = '960' AND type = 'profile';
UPDATE search_metadata SET text3 = 'unified school district' WHERE text2 = '970' AND type = 'profile';

-- Change ownership and add indexes to speed up search.

ALTER TABLE search_metadata OWNER TO census;
CREATE INDEX ON search_metadata (type);
CREATE INDEX ON search_metadata USING GIN(document);

-- Synonym support
UPDATE search_metadata SET document = document || to_tsvector('simple', coalesce('saint', ' ')) WHERE text1 LIKE '%St.%' AND type = 'profile';
UPDATE search_metadata SET document = document || to_tsvector('simple', coalesce('st', ' ')) WHERE text1 LIKE '%Saint%' AND type = 'profile';
UPDATE search_metadata SET document = document || to_tsvector('simple', coalesce('fort', ' ')) WHERE text1 LIKE '%Ft%' AND type = 'profile';
UPDATE search_metadata SET document = document || to_tsvector('simple', coalesce('ft', ' ')) WHERE text1 LIKE '%Fort%' AND type = 'profile';
UPDATE search_metadata SET document = document || to_tsvector('simple', coalesce('number', ' ')) WHERE text1 LIKE '%No.%' AND type = 'profile';
UPDATE search_metadata SET document = document || to_tsvector('simple', coalesce('no', ' ')) WHERE text1 LIKE '%Number%' AND type = 'profile';
UPDATE search_metadata SET document = document || to_tsvector('simple', coalesce('isd', ' ')) WHERE lower(text1) LIKE '%independent school district%' AND type = 'profile';

-- Support conventional short syntax for congressional districts
UPDATE search_metadata 
    SET document = document || to_tsvector('simple', coalesce(subquery.code, ' ')) 
    FROM
        (select regex.geoid, regex.match[2] || '-' || regex.match[1] as code from 
        (select text4 as geoid, regexp_matches(text1, 'Congressional District (\d+), (..)') as match 
         from search_metadata where type = 'profile' and text2 = '500') regex
    ) subquery
    WHERE search_metadata.text4 = subquery.geoid;

