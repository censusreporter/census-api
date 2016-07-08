-- Creates search_metadata table with columns 
-- -- text1: display_name or table_id,
-- -- text2: sumlevel or table_title,
-- -- text3: sumlevel_name or topics,
-- -- text4: full_geoid or simple_table_title,
-- -- type,
-- -- document
-- by pulling information about profiles (subquery before the UNION)
-- and about tables (subquery after the UNION). This creates just one metadata
-- table with all of the information we (currently) need for search.

CREATE TABLE search_metadata AS (
    SELECT CAST(display_name as text) AS text1,
           CAST(sumlevel as text) AS text2,
           NULL AS text3,
           CAST(full_geoid as text) as text4,
           'profile' AS type,
           document AS document
    FROM ( 
        SELECT display_name, sumlevel, full_geoid,
               setweight(to_tsvector(coalesce(display_name, ' ')), 'A') AS document
        FROM (
            SELECT DISTINCT display_name, sumlevel, full_geoid
            FROM tiger2014.census_name_lookup
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
    -- acs2014_1yr schema, with columns table_id, table_title, etc.,
    -- and document (the tsvector, the most important for search).
    --
    -- From this, we take the columns directly used in search results
    -- and put them into the combined metadata table.

    SELECT CAST(table_id as text) AS text1,
           CAST(table_title as text) AS text2,
           CAST(array_to_string(topics, ', ') as text) AS text3,
           CAST(simple_table_title as text) AS text4,
           'table' AS type,
           document AS document
    FROM ( 
        SELECT table_id, table_title, topics, simple_table_title,
               setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || 
               setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') || 
               setweight(to_tsvector(coalesce(string_agg(column_title, ' '), ' ')), 'C') || 
               setweight(to_tsvector(coalesce(universe, ' ')), 'D') as document 
        FROM (
            SELECT DISTINCT t.table_id, 
                            t.table_title,
                            t.simple_table_title,
                            t.topics,
                            t.subject_area,
                            t.universe,
                            c.column_title
            FROM acs2014_1yr.census_table_metadata t
            JOIN acs2014_1yr.census_column_metadata c
            ON t.table_id = c.table_id
            ) table_search

        WHERE table_id = table_search.table_id
        GROUP BY table_id, table_title, topics, subject_area, universe, table_search.simple_table_title
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