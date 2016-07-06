#!/usr/bin/env python
"""This would typically be used once after each database update to refresh the sitemap files.
   It has hard-coded the path where the files should be written, which is expected to be
   a checkout of the Census Reporter public webapp in a directory adjacent to this repository.

   It also has the database connect string hardcoded, because it will only get run once or twice a year.

"""
from table import write_table_sitemap
from profile import write_profile_sitemaps

DEFAULT_OUTPUT_DIR = '../../censusreporter/censusreporter/apps/census/static/sitemap/'
# this connect string uses a non-standard port, as in the case when something is being
# SSH tunneled from production. Fiddle with this as appropriate.
DEFAULT_CONNECT_STRING = 'postgresql://census:censuspassword@localhost:5433/census'
def main():
    write_table_sitemap(DEFAULT_OUTPUT_DIR,DEFAULT_CONNECT_STRING)
    write_profile_sitemaps(DEFAULT_OUTPUT_DIR,DEFAULT_CONNECT_STRING)
if __name__ == '__main__':
    main()
