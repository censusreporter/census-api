# -*- coding: utf-8 -*-

import psycopg2
import topic_scraper

""" table_tester.py
Check every table on every topic page for existence.

This script scrapes all of the topic pages (with the help of topic_scraper.py)
and looks at every table referenced on those pages. It looks up each one in
census_tabulation_metadata to see if it exists or not, and prints the results
of its search.
"""

def get_all_tables():
    """ Get a list of all tables in census_tabulation_metadata """

    # Connect to database
    connection = psycopg2.connect("dbname=census user=census")
    cur = connection.cursor()

    # Get all tables
    q = """SELECT tables_in_one_yr, tables_in_three_yr, tables_in_five_yr
           FROM census_tabulation_metadata;"""

    cur.execute(q)

    # Add all to list
    tables = []

    for one_yr, three_yr, five_yr in cur:
        tables += one_yr
        tables += three_yr
        tables += five_yr

    # Close connection
    cur.close()
    connection.close()

    return tables


def check_tables_exist(to_check, all_tables):
    """ Check if tables exist or not; return dict of which do and don't

    params: tables = dictionary of table codes : annotations, e.g.,
                     { code1: [ "‡", "†"], code2: [], ... }
            all_tables = list of all tables
    return: dictionary { "main_fine": [ all tables that properly exist ],
                         "mising": [ all tables that are missing ],
                         "no_collapsed": [ all tables that should have a
                                           C table, but don't ],
                         "no_iterations": [ all tables that should have racial
                                            iterations, but don't ],
                         "no_pr": [ all tables that should have a PR version,
                                    but don't ] }

    The annotations can include:
        ‡ - collapsed version exists
        † - has racial iterations
        § - has Puerto Rico version
        ª - no core table, only iterations (we don't do any checks for this)
    """

    results = { "main_fine": [], "missing": [], "no_collapsed": [],
                "no_iterations": [], "no_pr": [] }

    for table, annotations in to_check.iteritems():
        # Check existence of main table
        if table in all_tables:
            results["main_fine"].append(table)

        # Check for first racial iteration; if the base table doesn't
        # exist but iterations do, it will redirect, so those with
        # iterations should still count.
        elif table + u"A" in all_tables:
            results["main_fine"].append(table)

        # Otherwise, it's missing
        else:
            results["missing"].append(table)

        # Check for collapsed version
        if u"‡" in annotations:
            if u"C" + table[1:] not in all_tables:
                results["no_collapsed"].append(table)

        # Check for racial iterations
        if u"†" in annotations:
            if table + u"A" not in all_tables:
                results["no_iterations"].append(table)

        # Check for Puerto Rico version
        if u"§" in annotations:
            if table + u"PR" not in all_tables:
                results["no_pr"].append(table)

    return results


if __name__ == '__main__':
    topics = topic_scraper.get_list_of_topics()
    print("Obtained list of topics")

    all_tables = get_all_tables()
    print("Got list of all tables")

    for topic in topics:
        topic['text'], topic['tables'], topic['table_codes'] = topic_scraper.scrape_topic_page(**topic)
        print("Finished scraping topic page '{0}'".format(topic['name']))
        print(check_tables_exist(topic['tables'], all_tables))
