# Adding new data release

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

(If you're running under embargo, you can create these files but you will have to download the files from the embargo site manually and put them in e.g. /mnt/tmp/acs2013_1yr, then unzip them)

1. make a copy of a 02_download script and modify it for the new release.
    - Find and replace the year and release (e.g. `acs2014_3yr` -> `acs2015_1yr`)
2. make a copy of a 03_import script and modify it for the new release:
    - Find and replace the year and release (e.g. `acs2014_3yr` -> `acs2015_1yr`)
    - In this file there are some references to the year and release in a different format. Find and replace those, too. (e.g. `20143` -> `20151`)
    - Check the "Seq Table Number Lookup" .txt file (e.g. [this one for 2015_1yr](http://www2.census.gov/programs-surveys/acs/summary_file/2015/documentation/user_tools/ACS_1yr_Seq_Table_Number_Lookup.txt)) to see what the maximum sequence number is. It's usually the third column and has leading zeroes. Check that the number in the for loop around line 62 matches the max sequence number.
3. commit the update to git
4. Update the repository on the EC2 instance to bring in the new scripts
5. from the census-postgres-scripts dir on the EC2 instance, run:
    - ./02_download_acs_2013_3yr.sh

If this is a new release year, you'll want to set up the new TIGER geodata scripts, too:

1. Make a copy of a 12_download script and modify it for the new TIGER year
    - Find and replace the year
1. Make copies of the 13_import and 13_index scripts and modify them for the new TIGER year
    - Find and replace the year
    - In the 13_index script, update the join to ACS population estimate to a newer release year
1. Make a copy of the 14_aiannh script and modify it for the new TIGER year
    - Find and replace the year
1. Update and run the geocontainment_scripts/cbsa_containment.py script
    - It should point to the new release year and a new delineation URL
    - Run the containtment script (it will generate a new 15_cbsa script)
1. Commit the update to git
1. Update the repository on the EC2 instance to bring in the new scripts
1. from the census-postgres-scripts dir on the EC2 instance, run:
    - ./12_download_tiger_2018.sh
    - ./13_import_tiger_2018.sh
    - psql -d census -U census -f 13_index_tiger_2015.sql
    - psql -d census -U census -f 14_aiannh_tables_2018.sql
    - psql -d census -U census -f 15_cbsa_geocontainment_2018.sql
    - psql -d census -U census -c "drop schema tiger2018 cascade" (delete the old tiger data if the live API isn't using it)
1. TIGER errata:
    - ~A dozen of the TIGER 2018 geometries are invalid, so you need to fix them:
        psql -d census -U census -c "update tiger2018.census_name_lookup set geom=st_makevalid(geom) where not st_isvalid(geom);" ()
    - The CBSA delineation/containment data in the spreadsheet used above includes a reference to a non-existent CBSA, so the containment needs to be deleted:
        delete from tiger2018.census_geo_containment where child_geoid='31000US42460' and parent_geoid='33000US497';
1. Update the census-api api.py to add the new release to the `allowed_tiger` variable
1. Update the static website redirection rules for S3 bucket `embed.censusreporter.com` to add a section for the new TIGER release.

### Update census-postgres

(This chunk is mostly run on a remote EC2 instance because it involves downloading the raw data dumps from Census)

1. modify meta-scripts/build_sql_files.py:
    - add a new key in the `config` dict for the new release that looks like one of the other ones
    - check that the config data is correct by looking at the column names in the e.g. "[ACS_1yr_Seq_Table_Number_lookup.txt](http://www2.census.gov/programs-surveys/acs/summary_file/2015/documentation/user_tools/ACS_1yr_Seq_Table_Number_Lookup.txt)" file
    - commit the change to Github

- Double-check that the `02_download_acs_2013_3yr.sh` script ran on the EC2 instance:
  - Make sure you've downloaded the `Sequence_Number_and_Table_Number_Lookup.txt` file in `/mnt/tmp/acs2013_3yr`
  - Note that the Census sometimes will only release this as an XLS. If so:
        - Open the `Sequence_Number_and_Table_Number_Lookup.xls` file in Excel and save it as a CSV
        - Copy it to /mnt/tmp/acs2013_1yr on the EC2 instance you're using to build this
        - Make sure it's named .txt, not .csv
  - Make sure you've unzipped /mnt/tmp/acs2013_1yr/All_Geographies
        - (The 5yr release calls this `geog`, not `All_Geographies`)

- Check out (or pull down the new changes from above if it already exists) the census-postgres repo on the EC2 machine

- using census-postgres as your working dir:
  - mkdir acs2013_1yr
  - python meta-scripts/build_sql_files.py --working_dir=acs2013_1yr acs2013_1yr

- copy non-changing sql files from previous release to this one:
  - cd acs2020_5yr
  - cp ../acs2019_5yr/create_geoheader.sql \
         ../acs2019_5yr/create_tmp_geoheader.sql \
         ../acs2019_5yr/geoheader_comments.sql \
         ../acs2019_5yr/parse_tmp_geoheader.sql \
         ../acs2019_5yr/README.md \
         .

- update copied sql files to point to new release's schema
  - vi create_geoheader.sql # find/replace 2012 with 2013
  - vi create_tmp_geoheader.sql
  - vi geoheader_comments.sql
  - vi parse_tmp_geoheader.sql
  - vi README.md

- Since you probably checked out the census-postgres repo with https, you can't commit from the EC2 instance, so copy this data you just created back to your laptop:
  -
       ```
       scp -i ~/.ssh/censusreporter.ec2_key.pem -r \
        ubuntu@ec2-23-20-252-114.compute-1.amazonaws.com:/home/ubuntu/census-postgres/acs2013_3yr .
       ```
  - git add acs2013_3yr
  - git commit
  - git push
  - Once you do this, go back to the EC2 instance and rm the directory you made inside of census-postgres and pull it back down from git so you have a clean repo

### Import data to database

- using census-postgres-scripts as your working dir:
  - Make sure you have [a `.pgpass` file](https://www.postgresql.org/docs/9.1/static/libpq-pgpass.html) with your postgres database credentials in it so you don't have to type your password a dozen times in the import script
  - Set the PGHOST environment variable: `export PGHOST=censusreporter.redacted.us-east-1.rds.amazonaws.com`
  - Run `./03_import_acs_2013_1yr.sh`
  - You'll see a lot of NOTICEs flow by, but it's only important if it's an ERROR

### Update census-table-metadata

- Insert the table metadata (from the census-table-metadata repo)
  - Make sure your the census-table-metadata repo in `/home/ubuntu/census-table-metadata` is up to date
    - `cd /home/ubuntu/census-table-metadata`
    - `git pull`
  - Open a psql terminal: `psql -U census census` (it should connect using the `PGHOST` envvar from above)
    - Copy and execute in the psql terminal the CREATE TABLE and CREATE INDEX's for the new release from `census_metadata.sql`
    - Run the following in the psql terminal (adapted for your release):
      - `\copy acs2020_5yr.census_table_metadata  FROM '/home/ubuntu/census-table-metadata/precomputed/acs2020_5yr/census_table_metadata.csv' WITH csv ENCODING 'utf8' HEADER`
      - `\copy acs2020_5yr.census_column_metadata FROM '/home/ubuntu/census-table-metadata/precomputed/acs2020_5yr/census_column_metadata.csv' WITH csv ENCODING 'utf8' HEADER`

- Update the unified tabulation metadata (from the census-table-metadata repo)
  - Truncate the existing census_tabulation_metadata on the EC2 instance:
    - `truncate table census_tabulation_metadata;`
  - Copy the new data into the now-empty census_tabulation_metdata table:
    - `\copy census_tabulation_metadata FROM '/home/ubuntu/census-table-metadata/precomputed/unified_metadata.csv' WITH csv ENCODING 'utf8' HEADER`

- Create a new database dump
  - `pg_dump -h localhost -U census -n acs2014_1yr | gzip -c > acs2014_1yr_backup.sql.gz`
  - Upload it to S3 for safe keeping:
    - `aws s3 cp --region=us-east-1 --acl=public-read /mnt/tmp/acs2015_1yr_backup.sql.gz s3://census-backup/acs/2015/acs2015_1yr/acs2015_1yr_backup.sql.gz`
  - Update [the Tumblr post](http://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data) to include the new data dump

- Update the census-api `api.py` file to include the new release that's now in the database
  - Add the new release to the `allowed_releases` list. Usually you want to replace the existing release with the newer one you just imported. You might want to move the older release of the same type to the bottom of the list so that people can use it for specific requests.
  - If you are updating a 5yr release, you probably want to update the `default_table_search_release` variables, too.
  - Add an entry for the `ACS_NAMES` dict for the new release.
  - Commit the changes
  - Push to the `dokku.censusreporter.org` remote
    - `git push dokku`

- Update the Postgres full text index (from the EC2 instance)
  - `cd /home/ubuntu`
  - `git clone https://github.com/censusreporter/census-api.git`
  - `cd census-api`
  - Set the PGHOST, PGPORT, PGUSER, PGDATABASE environment variables
  - `psql -f /home/ubuntu/census-api/full-text-search/metadata_script.sql`
  - Scrape the topic pages:
        - `virtualenv --no-site-packages env`
        - `source env/bin/activate`
        - `pip install requests psycopg2-binary`
        - `python /home/ubuntu/census-api/full-text-search/topic_scraper.py`

- Regenerate the sitemap files
  - From a system which has the `census-api` and `censusreporter` repositories both checked out in the same parent directory, open an SSH tunnel to the database server tunneling on port `5433`
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
