#Full-Text Search Query Guide

acs2014_1yr is the schema of interest. Look first at census_table_metadata for
some sample table names and IDs.

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

The columns of interest are table_id and table_title:

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

Let's look at Median Age by Sex.

    census=# select * from acs2014_1yr.B01002 limit 5;

        geoid     | b01002001 | b01002002 | b01002003 
    --------------+-----------+-----------+-----------
     04000US02    |      33.3 |      32.8 |      33.7
     04001US02    |      31.9 |      30.9 |      33.1
     04043US02    |      36.5 |        37 |      35.8
     040A0US02    |      32.7 |      31.7 |      33.5
     05000US01055 |      40.4 |        39 |      43.1
     (5 rows)

There are three cryptic column names, but if we go to the table's page on 
Census Reporter (http://censusreporter.org/tables/B01002/), we see the 
actual names. These are stored in census_column_metadata.

    census=# select * from acs2014_1yr.census_column_metadata limit 5;

     table_id | line_number | column_id | column_title  | indent | parent_column_id 
    ----------+-------------+-----------+---------------+--------+------------------
     B00001   |         1.0 | B00001001 | Total         |      0 | 
     B00002   |         1.0 | B00002001 | Total         |      0 | 
     B01001   |         1.0 | B01001001 | Total:        |      0 | 
     B01001   |         2.0 | B01001002 | Male:         |      1 | B01001001
     B01001   |         3.0 | B01001003 | Under 5 years |      2 | B01001002
    (5 rows)

We see a column called table_id, so let's match it with the table ID from earlier.

    census=# select * from acs2014_1yr.census_column_metadata where table_id = 'B01002';

     table_id | line_number |  column_id  | column_title  | indent | parent_column_id 
    ----------+-------------+-------------+---------------+--------+------------------
     B01002   |         0.5 | B01002000.5 | Median age -- |      0 | 
     B01002   |         1.0 | B01002001   | Total:        |      1 | B01002000.5
     B01002   |         2.0 | B01002002   | Male          |      1 | B01002000.5
     B01002   |         3.0 | B01002003   | Female        |      1 | B01002000.5
    (4 rows)

And there's the information we wanted.

---

Looking at some ranodm tables in census_table_metadata, it appears that the 
most important table column will be table_title, followed by universe and then
subject_area. 

    census=# select table_id, subject_area, universe, table_title from acs2014_1yr.census_table_metadata order by random() limit 20;

Let's build a document vector out of those columns. This query:

    SELECT table_id, table_title, document FROM (
        SELECT table_id, table_title,
               to_tsvector(coalesce(table_title, ' ')) || 
               to_tsvector(coalesce(universe, ' ')) ||
               to_tsvector(coalesce(subject_area, ' ')) as document
        FROM acs2014_1yr.census_table_metadata
        ) as table_search
    LIMIT 10;

gives us unweighted document vectors for 10 tables. We can weight the columns
as described above:

    SELECT table_id, table_title, document FROM (
        SELECT table_id, table_title,
               setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || 
               setweight(to_tsvector(coalesce(universe, ' ')), 'B') ||
               setweight(to_tsvector(coalesce(subject_area, ' ')), 'C') as document
        FROM acs2014_1yr.census_table_metadata
        ) as table_search
    LIMIT 10;

Our first test case will be table B25074, "Household Income by Gross Rent as a 
Percentage of Household Income in the Past 12 Months." Currently, the search 
term "gross rent household income" does not return any results; with a full 
text search, this table should appear in the results.

This is turned into a query as to_tsquery('gross & rent & household & income').
We can run this to see all results:

    SELECT table_id, table_title FROM (
        SELECT table_id, table_title,
               setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || 
               setweight(to_tsvector(coalesce(universe, ' ')), 'B') ||
               setweight(to_tsvector(coalesce(subject_area, ' ')), 'C') as document
        FROM acs2014_1yr.census_table_metadata
        ) table_search
    WHERE table_search.document @@ to_tsquery('gross & rent & household & income');

and also see their relevancy:

    SELECT table_id, table_title, 
    ts_rank(table_search.document, to_tsquery('gross & rent & household & income')) as relevance 
    FROM (
        SELECT table_id, table_title,
               setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || 
               setweight(to_tsvector(coalesce(universe, ' ')), 'B') ||
               setweight(to_tsvector(coalesce(subject_area, ' ')), 'C') as document
        FROM acs2014_1yr.census_table_metadata
        ) table_search
    WHERE table_search.document @@ to_tsquery('gross & rent & household & income')
    ORDER BY relevance DESC;

which returns:

     table_id |                                        table_title                                         | relevance 
    ----------+--------------------------------------------------------------------------------------------+-----------
     B25074   | Household Income by Gross Rent as a Percentage of Household Income in the Past 12 Months   |         1
     B25072   | Age of Householder by Gross Rent as a Percentage of Household Income in the Past 12 Months |         1
     C25074   | Household Income by Gross Rent as a Percentage of Household Income in the Past 12 Months   |         1
     B25070   | Gross Rent as a Percentage of Household Income in the Past 12 Months                       |         1
     B25071   | Median Gross Rent as a Percentage of Household Income in the Past 12 Months (Dollars)      |         1
     B25122   | Household Income in the Past 12 Months (In 2014 Inflation-adjusted Dollars) by Gross Rent  |  0.999951
     C25122   | Household Income in the Past 12 Months (In 2014 Inflation-adjusted Dollars) by Gross Rent  |  0.999951
    (7 rows)

A simpler example is the search term "race sex" or to_tsquery('race & sex'), which
returns no results on the Census Reporter website, but, when substituted in the 
above query, returns 32 tables.

Note that if there are two tables that differ only in their alphabetic 
identifier (B##### vs. C#####), the 'C' table is a 'collapsed' version of the
'B' table, with fewer, less granular columns. 

The general query is:

    SELECT table_id, table_title, 
    ts_rank(table_search.document, to_tsquery('QUERY')) as relevance 
    FROM (
        SELECT table_id, table_title,
               setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || 
               setweight(to_tsvector(coalesce(universe, ' ')), 'B') ||
               setweight(to_tsvector(coalesce(subject_area, ' ')), 'C') as document
        FROM acs2014_1yr.census_table_metadata
        ) table_search
    WHERE table_search.document @@ to_tsquery('QUERY')
    ORDER BY relevance DESC;

---

Some example queries for testing:

- Gross rent housing income
- Race Sex
- Age income
- Family income by race