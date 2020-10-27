#!/usr/bin/env python
"""This would typically be used once after each database update to refresh the sitemap files.
   It has hard-coded the path where the files should be written, which is expected to be
   a checkout of the Census Reporter public webapp in a directory adjacent to this repository.

   It also has the database connect string hardcoded, because it will only get run once or twice a year.

"""
from table import write_table_sitemap
from profile import write_profile_sitemaps
import os


DEFAULT_OUTPUT_DIR = '../../censusreporter/censusreporter/apps/census/static/sitemap/'
# this connect string uses a non-standard port, as in the case when something is being
# SSH tunneled from production. Fiddle with this as appropriate.
DEFAULT_CONNECT_STRING = os.environ.get('DATABASE_URL')
def main():
    if DEFAULT_CONNECT_STRING is None:
        raise Exception("No database connect string. Set it using the DATABASE_URL env var.")
    write_table_sitemap(DEFAULT_OUTPUT_DIR,DEFAULT_CONNECT_STRING)
    write_profile_sitemaps(DEFAULT_OUTPUT_DIR,DEFAULT_CONNECT_STRING)
if __name__ == '__main__':
    main()
