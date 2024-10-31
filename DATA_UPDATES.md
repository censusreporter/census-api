# Adding new data release

## Updates to this document

- **2023-09-17**: Update for table-based ACS releases. Joe made these updates last year for 2021, but Ian is now updating this document during the process for 2022.

## Update census-table-metadata

1. Makefile

    - add new line in `all` section
    - add new `clean-all` step
    - add `census_table_metadata.csv` step

2. census_metadata.sql

    - add table/column metadata creates

3. census_metadata_drop.sql

    - add table/column metadata drops

4. census_metadata_load.sql

    - add COPYs

5. Handle errata with table metadata:

    - Discover that the shells .xls file they include is actually an .xlsx and that python xlrd can't read the formatting info (Use Excel to save it as a normal .xls and replace the file)
    - Discover that Census added a worksheet to the shells .xls file that is formatted completely differently (Use Excel to move Sheet2 before Sheet1)
    - The `ACS_1yr_Seq_Table_Number_Lookup.xls` for 2014 does not reflect the changes in [the new survey](https://www.census.gov/programs-surveys/acs/technical-documentation/table-and-geography-changes/2014/1-year.html), but the [text/CSV version](http://www2.census.gov/programs-surveys/acs/summary_file/2014/documentation/user_tools/ACS_1yr_Seq_Table_Number_Lookup.txt) does so I converted it to an XLS with Excel so that the rest of my existing process would work
    - The 2018 1-yr and 5-yr releases included a `ACS_1yr_Seq_Table_Number_Lookup.csv` and no `.xls` version. I converted it to an XLS with Excel so that the rest of my existing process would work
    - Starting with 2019 1-yr, Census stopped including indent information, so it fetched from the Census API with process_api.py now. No Excel sheets need to be processed.
    - Starting with 2022 1-yr, we switched to using the table-based release, since Census stopped releasing the sequence-based release. 
    - the TIGER2022 release did not update include CBSA, CSA, or METDIV shapefiles for some reason. We skipped them, but that led to data problems. In the future, we should carry forward missing geographies to the next TIGER release unless we have a clear reason not to.
    - in 2022, some time after our load, Census added 2020 versions of the UAC shapefile, which are the ones we need for ACS2022. We had to go back and fix it later. A naive repeat of our process would probably download both the 2010 and 2020 UAC files, and then would run into trouble because the schemas use slightly different column names.

6. Generate the 'precomputed' metadata stuff. From census-table-metadata:
    - pipenv install && pipenv shell
    - make
    - git add precomputed/acs2018_1yr
    - git commit
    - git push

7. Update the `unified_metadata.csv`:
    - Update the `releases_to_analyze` variable in `analyze_metadata.py` to include the new release
    - python analyze_metadata.py
    - git add precomputed/unified_metadata.csv
    - git commit
    - git push

## Update census-postgres-scripts

1. make a copy of a table_based/02_download script and modify it for the new release.
    - Find and replace the year and release (e.g. `acs2021_1yr` -> `acs2022_1yr`)
2. make a copy of a table_based/03_import script and modify it for the new release:
    - Find and replace the year and release (e.g. `acs2021_1yr` -> `acs2022_1yr`)
    - In this file there are some references to the year and release in a different format. Find and replace those, too. (e.g. `20211` -> `20221`)
3. commit the update to git
4. Update the repository on the EC2 instance to bring in the new scripts
5. from the census-postgres-scripts dir on the EC2 instance, run:
    - cd table_based
    - ./02_download_acs_2022_1yr.sh

If this is a new release year, you'll want to set up the new TIGER geodata scripts, too:

1. Make a copy of a table_based/12_download script and modify it for the new TIGER year
    - Find and replace the year
1. Make copies of the table_based/13_import and table_based/13_index scripts and modify them for the new TIGER year
    - Find and replace the year
    - In the 13_index script, update the join to ACS population estimate to a newer release year
1. Make a copy of the table_based/14_aiannh script and modify it for the new TIGER year
    - Find and replace the year
1. Update and run the geocontainment_scripts/cbsa_containment.py script
    - It should point to the new release year and a new delineation URL
    - Run the containtment script (it will generate a new 15_cbsa script)
    - (This doesn't exist for 2021?)
1. Commit the update to git
1. Update the repository on the EC2 instance to bring in the new scripts
1. Set the PGHOST, PGPORT, PGUSER, and PGPASSWORD environment variables
1. from the census-postgres-scripts dir on the EC2 instance, run:
    - ./12_download_tiger_2022.sh
    - ./13_import_tiger_2022.sh
    - psql -d censusreporter -f 13_index_tiger_2022.sql
    - psql -d censusreporter -f 14_aiannh_tables_2022.sql
    - psql -d censusreporter -f 15_cbsa_geocontainment_2022.sql
    - psql -d censusreporter -c "drop schema tiger2021 cascade" (delete the old tiger data if the live API isn't using it)
1. TIGER errata:
    - ~A dozen of the TIGER 2018 geometries are invalid, so you need to fix them:
        psql -d censusreporter -c "update tiger2018.census_name_lookup set geom=st_makevalid(geom) where not st_isvalid(geom);" ()
    - The CBSA delineation/containment data in the spreadsheet used above includes a reference to a non-existent CBSA, so the containment needs to be deleted:
        delete from tiger2018.census_geo_containment where child_geoid='31000US42460' and parent_geoid='33000US497';
    - The TIGER 2022 PLACE theme has two columns that shp2pgsql resolves to `varchar(0)`, which is invalid and will be filtered out.
1. Update the census-api api.py to add the new release to the `allowed_tiger` variable
1. (not needed with switch to Cloudflare in 2024!) Update the static website redirection rules for S3 bucket `embed.censusreporter.com` to add a section for the new TIGER release.

### Update census-postgres

(Note that this section is a fairly major rewrite for switching to table-based releases in 2022.)

1. Make a new directory for your new release in the census-postgres directory
    - `mkdir -p acs2022_1yr`
2. Download the table shells file for your release.
    - `curl -o acs2022_1yr/ACS20221YR_Table_Shells.txt https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/ACS20221YR_Table_Shells.txt`
3. Run the `build_table_based_sql.py` script to generate the SQL files for your release.
    - `python meta-scripts/build_table_based_sql.py acs2022_1yr/ACS20221YR_Table_Shells.txt acs2022_1yr acs2022_1yr`
4. Copy the `create_geoheader.sql` file from the previous release to the directory for the new release. Edit it to update the year and release.
    - Make sure you're copying from a release that has the `logrecno` column removed, as the geoheader columns changed as part of the switch to table-based releases.
    - `cp acs2021_1yr/create_geoheader.sql acs2022_1yr`
5. Commit the new release directory to git and push it to Github.
    - `git add acs2022_1yr`
    - `git commit`
    - `git push`
6. Update the census-postgres repo on the remote instance to bring in the new release.

### Import data to database

1. Make sure you ran the `02_download_acs_2022_1yr.sh` script on the remote instance in the steps above.
2. Adjust the geoids in the downloaded data to match the expected format:
    - `python3 meta-scripts/fix_geoids.py /home/ubuntu/data/acs2022_1yr/`
3. Set the `PGURI` environment variable to point to the database you want to import to.
    - `export PGURI=postgres://postgres:@localhost:8421/censusreporter`
4. Import the data to the database:
    - `cd /home/ubuntu/census-postgres-scripts/table_based`
    - `./03_import_acs_2022_1yr.sh`

### Update census-table-metadata

- Insert the table metadata (from the census-table-metadata repo)
  - Make sure your the census-table-metadata repo in `/home/ubuntu/census-table-metadata` is up to date
    - `cd /home/ubuntu/census-table-metadata`
    - `git pull`
  - Open a psql terminal: `psql $PGURI` (it should connect using the `PGURI` envvar from above)
    - Copy and execute in the psql terminal the CREATE TABLE and CREATE INDEX's for the new release from `census_metadata.sql`
    - Run the following in the psql terminal (adapted for your release):
      - `\copy acs2023_1yr.census_table_metadata FROM '/home/ubuntu/census-table-metadata/precomputed/acs2023_1yr/census_table_metadata.csv' WITH csv ENCODING 'utf8' HEADER`
      - `\copy acs2023_1yr.census_column_metadata FROM '/home/ubuntu/census-table-metadata/precomputed/acs2023_1yr/census_column_metadata.csv' WITH csv ENCODING 'utf8' HEADER`

- Update the unified tabulation metadata (from the census-table-metadata repo)
  - Truncate the existing census_tabulation_metadata on the EC2 instance:
    - `truncate table census_tabulation_metadata;`
  - Copy the new data into the now-empty census_tabulation_metdata table:
    - `\copy census_tabulation_metadata FROM '/home/ubuntu/census-table-metadata/precomputed/unified_metadata.csv' WITH csv ENCODING 'utf8' HEADER`

- Create a new database dump
  - `pg_dump -n acs2023_1yr | gzip -c > /home/ubuntu/data/acs2023_1yr_backup.sql.gz`
  - Upload it to S3 for safe keeping:
    - `aws s3 cp --region=us-east-1 --acl=public-read /home/ubuntu/data/acs2023_1yr_backup.sql.gz s3://census-backup/acs/2023/acs2023_1yr/acs2023_1yr_backup.sql.gz`
  - Update [the Tumblr post](http://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data) to include the new data dump

- Update the census-api `api.py` file to include the new release that's now in the database
  - Add the new release to the `allowed_releases` list. Usually you want to replace the existing release with the newer one you just imported. You might want to move the older release of the same type to the bottom of the list so that people can use it for specific requests.
  - If you are updating a 5yr release, you probably want to update the `default_table_search_release` variables, too.
  - Add an entry for the `ACS_NAMES` dict for the new release.
  - Commit the changes
  - Push to the `dokku.censusreporter.org` remote
    - `git push dokku`

- Update the Postgres full text index (from the EC2 instance)
  - review and update the schemas in `full-text-search/metadata_script.sql`; commit any changes
  - `cd /home/ubuntu`
  - `git clone https://github.com/censusreporter/census-api.git`
  - `cd census-api`
  - Set the PGHOST, PGPORT, PGUSER, PGDATABASE environment variables
  - `psql -f /home/ubuntu/census-api/full-text-search/metadata_script.sql`
  - Scrape the topic pages:
        - activate a `census-api` environment
        - ensure EXTRACTOMATIC_CONFIG_MODULE and DATABASE_URL env vars are set correctly (topic_scraper uses the Flask API)
        - `python -m census_extractomatic.tools.topic_scraper`
  - Update the priority weighting. 
        - activate a `census-api` environment
        - `python -m census_extractomatic.tools.update_table_priorities FILES` (where FILES is a list or glob of gzip'd Census Reporter access logs as found on dokku at `/var/log/nginx/censusreporter-access*gz`)

- Regenerate the sitemap files
  - From a system which has the `census-api` and `censusreporter` repositories both checked out in the same parent directory, open an SSH tunnel to the database server tunneling on port `5433`
  - activate the `census-api` virtual environment
  - change to the `census-api/sitemap` directory
  - execute `python build_all.py`
  - `cd ../../censusreporter`
  - commit the new/updated Sitemap files to Git and deploy them as part of the `censusreporter` app

- Update the `censusreporter` Django app to use a new set of keys for profile pages
  - If this is a 1yr release (meaning we now have a previous year's 5yr release and a current year's 1yr release) then we'll only change the S3 key around [this line of code](https://github.com/censusreporter/censusreporter/blob/6ac6de2/censusreporter/apps/census/views.py#L411-L412) to read the current year's ACS release instead of the previous year's.
  - If this is a 5yr release (meaning we now have a 1yr *and* a 5yr release for the current year in the database) then we shouldn't need to make any changes to the S3 key, but we do need to clear out the S3 keys for the current year. This will force us to re-create existing datasets with the possibly-newer data that was just added.

- After embargo, remember to check in your work:
  - census-postgres/acs2013_1yr
  - census-table-metadata/precomputed/acs2013_1yr

- Clean up after yourself:
  - Don't forget to kill any extra EC2 instances you might have created for this process
