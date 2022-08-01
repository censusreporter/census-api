To process an aggregation request:

- Upload the geography using https://censusreporter.org/user_geo/
    - If a public URL is available, leave the "shared" checkbox checked, but if not, uncheck it.
- in the `census-api` environment
    - open an SSH tunnel to the Census Reporter database
    - run `python -m census_extractomatic.aggregation.test` 
    - for any fully READY datasets that aren't already in the timing.log file, it will generate all of the downloadables, avoiding the timeout issue when triggering the generation via a "just-in-time" click
- If the upload aligns with a Census PLACE
    - update audit_guide.csv with a stub row that matches the user_geo hash with the place FIPS code
    - switch to the `data` env (or at least something that has the right deps, which census-api doesn't)
    - run `python -m census_extractomatic.aggregation.audit`
