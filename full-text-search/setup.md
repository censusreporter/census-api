# Full Text Search Setup
This guide is intended to provide instructions for configuring full text search on Census Reporter. Broadly speaking, there are three levels of complexity to be aware of when setting this up: the database, the API, and the front end.

## Database Setup
We create a shared metadata table, `search_metadata` that indexes information about both profiles and tables. The SQL script that creates this is found in `census-api/full-text-search/metadata_script.sql`. To execute, run
	psql census < metadata_script.sql
and allow it to finish. Note that this may take a while, because it indexes all of the place names.

**Notes**: This script requires the presence of tiger2014.census_name_lookup, acs2014_1yr.census_column_metadata, and census_tabulation_metadata. It does not access any other tables. It will likely need to be modified if these tables are not present or to index tigeer data from other years, 3 year data, etc.

## API Setup
Fortunately, very little setup for the API is required. The version of `census-api/census_extractomatic/api.py` in the branch `full-text-search` contains the necessary functions in `full_text_search`. The API can be run locally and found at the route `/2.1/full-text/search`. For example, run
	python census_extractomatic/api.py
and query it directly as
	http://0.0.0.0:5000/2.1/full-text/search?q=puerto rico
to view results.

## Frontend Setup
Finally, as before, little is required for the front end. The necessary files can be found in `censusreporter/censusreporter/apps/census/static/js/search-results.js`, `.../census/static/js/full-text.search.js`, `.../census/templates/search/results.html`, and `.../census/templates/full_text_search.html`.

It is also necessary to point the API towards the locally running version. To do this, go to `censusreporter/censusreporter/config/base/settings.py` and update the line with `API_URL`
	API_URL = 'http://0.0.0.0:5000'
(and optionally comment the old one). Run censusreporter locally with
	python manage.py runserver
and find the full text search at `http://127.0.0.1:8000/full-text-search/`.