#Full-Text Search Query Guide
## Explanation of SQL querying for tabulation data

In order to implement a full-text search, we need to access table data beyond their IDs. This data is stored in the schema `acs2014_1yr`, particularly in the tables `census_table_metadata` and `census_column_metadata`. Look at the former for some sample table names and IDs. 

    census=# \d acs2014_1yr.census_table_metadata

             Table "acs2014_1yr.census_table_metadata"
            Column         |         Type          | Modifiers 
    -----------------------+-----------------------+-----------
     table_id              | character varying(10) | not null
     table_title           | text                  | 
     simple_table_title    | text                  | 
     subject_area          | text                  | 
     universe              | text                  | 
     denominator_column_id | character varying(16) | 
     topics                | text[]                | 
    Indexes:
        "census_table_metadata_pkey" PRIMARY KEY, btree (table_id)
        "census_table_metadata_lower_idx" btree (lower(table_title) text_pattern_ops)
        "census_table_metadata_topics_idx" gin (topics)

The columns of interest to us are `table_id` and `table_title`:

    census=# select table_id, table_title from acs2014_1yr.census_table_metadata limit 20;
     
     table_id |                             table_title                              
    ----------+----------------------------------------------------------------------
     B00001   | Unweighted Sample Count of the Population
     B00002   | Unweighted Sample Housing Units
     B01001   | Sex by Age
     B01001A  | Sex by Age (White Alone)
     B01001B  | Sex by Age (Black or African American Alone)
     B01001C  | Sex by Age (American Indian and Alaska Native Alone)
     B01001D  | Sex by Age (Asian Alone)
     B01001E  | Sex by Age (Native Hawaiian and Other Pacific Islander Alone)
     B01001F  | Sex by Age (Some Other Race Alone)
     B01001G  | Sex by Age (Two or More Races)
     B01001H  | Sex by Age (White Alone, Not Hispanic or Latino)
     B01001I  | Sex by Age (Hispanic or Latino)
     B01002   | Median Age by Sex
     B01002A  | Median Age by Sex (White Alone)
     B01002B  | Median Age by Sex (Black or African American Alone)
     B01002C  | Median Age by Sex (American Indian and Alaska Native)
     B01002D  | Median Age by Sex (Asian Alone)
     B01002E  | Median Age by Sex (Native Hawaiian and Other Pacific Islander Alone)
     B01002F  | Median Age by Sex (Some Other Race Alone)
     B01002G  | Median Age by Sex (Two or More Races)
    (20 rows)

Choose any table, e.g., Median Age by Sex, with table ID B01002.

    census=# select * from acs2014_1yr.B01002 limit 5;

        geoid     | b01002001 | b01002002 | b01002003 
    --------------+-----------+-----------+-----------
     04000US02    |      33.3 |      32.8 |      33.7
     04001US02    |      31.9 |      30.9 |      33.1
     04043US02    |      36.5 |        37 |      35.8
     040A0US02    |      32.7 |      31.7 |      33.5
     05000US01055 |      40.4 |        39 |      43.1
     (5 rows)

There are three cryptic column names, but if we go to the table's page on Census Reporter (http://censusreporter.org/tables/B01002/), we see the actual names. These are what is stored in `census_column_metadata`.

    census=# select * from acs2014_1yr.census_column_metadata limit 5;

     table_id | line_number | column_id | column_title  | indent | parent_column_id 
    ----------+-------------+-----------+---------------+--------+------------------
     B00001   |         1.0 | B00001001 | Total         |      0 | 
     B00002   |         1.0 | B00002001 | Total         |      0 | 
     B01001   |         1.0 | B01001001 | Total:        |      0 | 
     B01001   |         2.0 | B01001002 | Male:         |      1 | B01001001
     B01001   |         3.0 | B01001003 | Under 5 years |      2 | B01001002
    (5 rows)

This looks like a list of column IDs and titles, each with their corresponding table ID. Match the `table_id` column with the table ID we looked up earlier.

    census=# select * from acs2014_1yr.census_column_metadata where table_id = 'B01002';

     table_id | line_number |  column_id  | column_title  | indent | parent_column_id 
    ----------+-------------+-------------+---------------+--------+------------------
     B01002   |         0.5 | B01002000.5 | Median age -- |      0 | 
     B01002   |         1.0 | B01002001   | Total:        |      1 | B01002000.5
     B01002   |         2.0 | B01002002   | Male          |      1 | B01002000.5
     B01002   |         3.0 | B01002003   | Female        |      1 | B01002000.5
    (4 rows)

Here is the information we wanted -- we have column titles for every column in the table.

---

To implement the full text search, we should first understand the data out of which we will build our document. Luckily, Postgres makes this very simple, and allows us to give different fields different 'weights' when building a document. We will give `table_title` the highest weight, followed in order by `subject_area`, the column names, and `universe`.

The `table_title`, `subject_area`, and `universe` columns are easy enough to extract, for a given table, from `census_table_data` with a single query. What complicates this is the fact that each table has multiple column names, and these column names are stored in a separate table.

Ignoring the column names momentarily, we can build a document vector out of those columns. This query:

    SELECT table_id, table_title, document FROM (
        SELECT table_id, table_title,
               to_tsvector(coalesce(table_title, ' ')) || 
               to_tsvector(coalesce(subject_area, ' ')) ||
               to_tsvector(coalesce(universe, ' ')) as document
        FROM acs2014_1yr.census_table_metadata
        ) as table_search
    LIMIT 10;

returns unweighted document vectors for 10 tables, using the three columns described above. We can weight the columns as well:

    SELECT table_id, table_title, document FROM (
        SELECT table_id, table_title,
               setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || 
               setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') ||
               setweight(to_tsvector(coalesce(universe, ' ')), 'C') as document
        FROM acs2014_1yr.census_table_metadata
        ) as table_search
    LIMIT 10;

Our next task is to add column names data to these documents. First, we need data about all of the column names for a given table. Consider table ID C24050, which contains data about industries of the employed population. We can view all of the distinct column names using this query:

    census=# SELECT DISTINCT c.column_title 
                    FROM acs2014_1yr.census_table_metadata t  
                    JOIN acs2014_1yr.census_column_metadata c 
                    ON t.table_id = c.table_id WHERE t.table_id = 'C24050';

                                            column_title
    ---------------------------------------------------------------------------
     Information
     Finance and insurance, and real estate and rental and leasing
     Retail trade
     Agriculture, forestry, fishing and hunting, and mining
     Total:
     Production, transportation, and material moving occupations:
     Construction
     Manufacturing
     Transportation and warehousing, and utilities
     Professional, scientific, and management, and administrative and waste management services
     Arts, entertainment, and recreation, and accommodation and food services
     Other services, except public administration
     Public administration
     Natural resources, construction, and maintenance occupations:
     Educational services, and health care and social assistance
     Management, business, science, and arts occupations:
     Service occupations:
     Wholesale trade
     Sales and office occupations:
    (19 rows)

We can select other relevant information as well:

    census=# SELECT DISTINCT t.table_id, t.table_title, t.subject_area, 
                    t.universe, c.column_title 
                FROM acs2014_1yr.census_table_metadata t  
                JOIN acs2014_1yr.census_column_metadata c 
                ON t.table_id = c.table_id WHERE t.table_id = 'C24050';

Our next task is to turn the table of column names into one row, which we can then transform into a document. We do this with the string_agg function; refer to the Postgres documentation for how this works, but, in essence, it aggregates a column of results into a string of the entries. Similar to what was done above, we can create our document vectors and weight them appropriately:

    SELECT table_id, table_title, 
        setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || 
        setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') || 
        setweight(to_tsvector(coalesce(string_agg(column_title, ' '), ' ')), 'C') || 
        setweight(to_tsvector(coalesce(universe, ' ')), 'D') as document 
    FROM (
        SELECT DISTINCT t.table_id, t.table_title, t.subject_area, 
            t.universe, c.column_title 
        FROM acs2014_1yr.census_table_metadata t  
        JOIN acs2014_1yr.census_column_metadata c 
        ON t.table_id = c.table_id) table_search
    WHERE table_id = table_search.table_id 
    GROUP BY table_id, table_title, subject_area, universe;

Since the innermost query returns information about all columns in all tables (table_search), and the outer query is only concerned with one table, we use the final WHERE clause to filter our results into one record that represents one table.

Finally, we can display the relevance of results using ts_rank.

    SELECT table_id, table_title, 
           ts_rank(table_info.document, to_tsquery('postmasters')) as relevance
    FROM (
        SELECT table_id, table_title, 
            setweight(to_tsvector(coalesce(table_title, ' ')), 'A') ||
            setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') ||
            setweight(to_tsvector(coalesce(string_agg(column_title, ' '), ' ')), 'C') ||
            setweight(to_tsvector(coalesce(universe, ' ')), 'D') as document
        FROM (
            SELECT DISTINCT t.table_id, t.table_title, t.subject_area,
                            t.universe, c.column_title
            FROM acs2014_1yr.census_table_metadata t 
            JOIN acs2014_1yr.census_column_metadata c
            ON t.table_id = c.table_id) table_search
        WHERE table_id = table_search.table_id
        GROUP BY table_id, table_title, subject_area, universe
        ) table_info
    WHERE table_info.document @@ to_tsquery('postmasters')
    ORDER BY relevance DESC;

---

To test this, consider the table B25074, "Household Income by Gross Rent as a Percentage of Household Income in the Past 12 Months." Currently, the search term "gross rent household income" does not return any results; with a full text search, this table should appear in the results.

    census=# SELECT table_id, table_title, 
    census-#        ts_rank(table_info.document, to_tsquery('gross & rent & household & income')) as relevance FROM (
    census(#     SELECT table_id, table_title, 
    census(#         setweight(to_tsvector(coalesce(table_title, ' ')), 'A') ||
    census(#         setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') ||
    census(#         setweight(to_tsvector(coalesce(string_agg(column_title, ' '), ' ')), 'C') ||
    census(#         setweight(to_tsvector(coalesce(universe, ' ')), 'D') as document
    census(#     FROM (
    census(#         SELECT DISTINCT t.table_id, t.table_title, t.subject_area,
    census(#                         t.universe, c.column_title
    census(#         FROM acs2014_1yr.census_table_metadata t 
    census(#         JOIN acs2014_1yr.census_column_metadata c
    census(#         ON t.table_id = c.table_id) table_search
    census(#     WHERE table_id = table_search.table_id
    census(#     GROUP BY table_id, table_title, subject_area, universe
    census(#     ) table_info
    census-# WHERE table_info.document @@ to_tsquery('gross & rent & household & income')
    census-# ORDER BY relevance DESC;

     table_id |                                        table_title                                         | relevance 
    ----------+--------------------------------------------------------------------------------------------+-----------
     B25071   | Median Gross Rent as a Percentage of Household Income in the Past 12 Months (Dollars)      |         1
     B25074   | Household Income by Gross Rent as a Percentage of Household Income in the Past 12 Months   |         1
     C25074   | Household Income by Gross Rent as a Percentage of Household Income in the Past 12 Months   |         1
     B25072   | Age of Householder by Gross Rent as a Percentage of Household Income in the Past 12 Months |         1
     B25070   | Gross Rent as a Percentage of Household Income in the Past 12 Months                       |         1
     C25122   | Household Income in the Past 12 Months (In 2014 Inflation-adjusted Dollars) by Gross Rent  |  0.999974
     B25122   | Household Income in the Past 12 Months (In 2014 Inflation-adjusted Dollars) by Gross Rent  |   0.99997
    (7 rows)

Note that if there are two tables that differ only in their alphabetic identifier (B##### vs. C#####), the 'C' table is a 'collapsed' version of the 'B' table, with fewer, less granular columns. 