CREATE TABLE TABLE_SEARCH_METADATA AS (
	SELECT table_id, table_title, simple_table_title,
			topics,	universe, 
			setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || 
			setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') || 
			setweight(to_tsvector(coalesce(string_agg(column_title, ' '), ' ')), 'C') || 
			setweight(to_tsvector(coalesce(universe, ' ')), 'D') as document 
	FROM (
		SELECT DISTINCT t.table_id, 
						t.table_title, 
						t.subject_area,
						t.universe,
						t.simple_table_title,
						t.topics,
						c.column_title
		FROM acs2014_1yr.census_table_metadata t
		JOIN acs2014_1yr.census_column_metadata c
		ON t.table_id = c.table_id) table_search
	WHERE table_id = table_search.table_id
	GROUP BY table_id, table_title, simple_table_title, 
				topics, universe, subject_area
	)
;

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
--
-- Outermost query transforms relevant information into a document,
-- i.e., a tsvector, by using the table title and other data.
-- coalesce is used because the columns have the potential to
-- have null values. string_agg creates one string of all
-- column names. For full detail, refer to the psql docs.
--
-- This creates a table with one row for every table in the 
-- acs2014_1yr schema, with columns table_id, table_title, etc.,
-- and document (the tsvector, the most important for search).