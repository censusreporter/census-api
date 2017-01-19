# Abbreviations Guide
## Instructions to add support for abbreviations in search
Reference: [Postgres documentation](https://www.postgresql.org/docs/current/static/textsearch-dictionaries.html)

Our goal is to add support for abbreviations in Postgres -- that is, treat "st" and "saint" equivalently, and likewise with "fort" and "ft" or "no" and "number". We also want the user to be unaware that anything has changed; but if they search for "Saint Louis," St. Louis, MO should be among the results.

To do this, we make use of a Postgres feature called a thesaurus. This allows us to specify additional lexemes to index when a particular word is encountered; for example, the document for the place St. Louis will then contain the lexemes "saint", "st", and "louis".

Go to the `tsearch_data` directory under your shared data directory. For Linux, you will most likely find the shared data directory at `usr/share/postgresql/9.5/`, and for OS X at `/Applications/Postgres.app/Contents/Versions/9.4/share/postgresql/`. Make a new file in `tsearch_data` called `place_thesaurus.ths`. In it, include the following:

    saint : saint st
    st : saint st
    fort : fort ft 
    ft : fort ft
    no : number no
    number : number no
    
This is a dictionary of keys (left column) and lexemes to parse the key into. We now need to tell Postgres to refer to this dictionary. Run `psql census` and these commands:

	CREATE TEXT SEARCH DICTIONARY place_thesaurus (
        TEMPLATE = thesaurus, 
        DictFile = place_thesaurus,
        Dictionary = simple
    );
    
    ALTER TEXT SEARCH CONFIGURATION simple 
    ALTER MAPPING for asciiword 
    WITH place_thesaurus, simple;

You can verify that this works by running these tests:

    SELECT * FROM ts_debug('simple', 'st & louis');
    SELECT * FROM ts_debug('simple', 'ft & lauderdale');
    
Finally, we have to rebuild the `search_metadata` table to utilize this new indexing. Simply run `psql census < metadata_script.sql` from your terminal, and you will be able to see the changes reflected in search results.