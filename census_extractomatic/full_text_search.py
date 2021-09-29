from math import log10, log
import re

def do_search(db, q, object_type, limit):
    """ Search for objects (profiles, tables, topics) matching query q.

    Return a list, because it's easier to work with than a SQLAlchemy
    ResultProxy object (notably, the latter does not support indexing).
    """

    if object_type == 'profile':
        query = """SELECT text1 AS display_name,
                            text2 AS sumlevel,
                            text3 AS sumlevel_name,
                            text4 AS full_geoid,
                            text5 AS population,
                            text6 AS priority,
                            ts_rank(document, to_tsquery('simple', :search_term)) AS relevance,
                            type
                    FROM search_metadata
                    WHERE document @@ to_tsquery('simple', :search_term)
                    AND type = 'profile'
                    ORDER BY CAST(text6 as INT) ASC,
                                CAST(text5 as INT) DESC,
                                relevance DESC
                    LIMIT :limit;"""

    elif object_type == 'table':
        query = """SELECT text1 AS tabulation_code,
                            text2 AS table_title,
                            text3 AS topics,
                            text4 AS simple_table_title,
                            text5 AS tables,
                            cast(text6 AS INT) AS priority,
                            ts_rank(document, to_tsquery(:search_term), 2|8|32) AS relevance,
                            type
                    FROM search_metadata
                    WHERE document @@ to_tsquery(:search_term)
                    AND type = 'table'
                    ORDER BY priority DESC, relevance DESC
                    LIMIT :limit;"""

    elif object_type == 'topic':
        query = """SELECT text1 as topic_name,
                            text3 as url,
                            ts_rank(document, plainto_tsquery(:search_term)) AS relevance,
                            type
                    FROM search_metadata
                    WHERE document @@ plainto_tsquery(:search_term)
                    AND type = 'topic'
                    ORDER BY relevance DESC
                    LIMIT :limit;"""

    objects = db.session.execute(query, {"search_term": q, "limit": limit})
    return [row for row in objects]

def compute_score(row):
    """ Compute a ranking score in range [0, 1] from a row result.

    params: row - SQLAlchemy RowProxy object, which is returned by queries
    return: score in range [0, 1]
    """

    object_type = row['type']

    # Topics; set somewhat-arbitrary cutoff for PSQL relevance, above which
    # the result should appear first, and below which it should simply be
    # multiplied by some amount to make it appear slightly higher

    if object_type == 'topic':
        relevance = row['relevance']

        if relevance > 0.4:
            return 1

        else:
            return relevance * 2

    # Tables; take the PSQL relevance score, which (from our testing)
    # appears to always be in the range [1E-8, 1E-2]. For safety, we
    # generalize that to [1E-9, 1E-1] (factor of 10 on each side).
    #
    # The log sends [1E-9, 1E-1] to [-9, -1]; add 9 to send it to [0, 8];
    # divide by 8 to send it to [0, 1].

    elif object_type == 'table':
        relevance = (log10(row['relevance']) + 9) / 8.0
        TABLE_PRIORITY_RANGE = 100
        try:            
            raw_priority = row['priority'] if row['priority'] else 0
        except KeyError:
            print(f"No priority for {row['tabulation_code']} {row['simple_table_title']}")
            raw_priority = 0
        priority = raw_priority / TABLE_PRIORITY_RANGE
        score = priority * 0.5 + relevance * 0.5
        return score

    # Profiles; compute score based off priority and population. In
    # general, larger, more populous areas should be returned first.

    elif object_type == 'profile':
        priority = row['priority']
        population = row['population']

        # Priority bounds are 5 (nation) to 320 (whatever the smallest one
        # is), so the actual range is the difference, 315.
        PRIORITY_RANGE = 320.0 - 5

        # Approximate value, but realistically it shouldn't matter much.
        POP_US = 318857056.0

        # Make population nonzero (catch both empty string and string '0')
        if not population or not int(population):
            population = 1

        priority, population = int(priority), int(population)

        # Decrement priority by 5, to map [5, 320] to [0, 315].
        priority -= 5

        # Since priority is now in [0, 315], and PRIORITY_RANGE = 315, the
        # function (1 - priority / PRIORITY_RANGE) sends 0 -> 0, 315 -> 1.
        # Similarly, the second line incorporating population maps the range
        # [0, max population] to [0, 1].
        #
        # We weight priority more than population, because from testing it
        # gives the most relevant results; the 0.8 and 0.2 can be tweaked
        # so long as they add up to 1.
        return ((1 - priority / PRIORITY_RANGE) * 0.8 +
                (1 + log(population / POP_US) / log(POP_US)) * 0.2)

def choose_table(tables):
    """ Choose a representative table for a list of table_ids.

    In the case where a tabulation has multiple iterations / subtables, we
    want one that is representative of all of them. The preferred order is:
        'C' table with no iterations
        > 'B' table with no iterationks
        > 'C' table with iterations (arbitrarily choosing 'A' iteration)
        > 'B' table with iterations (arbitrarily choosing 'A' iteration)
    since, generally, simpler, more complete tables are more useful. This
    function selects the most relevant table based on the hierarchy above.

    Table IDs are in the format [B/C]#####[A-I]. The first character is
    'B' or 'C', followed by five digits (the tabulation code), optionally
    ending with a character representing that this is a race iteration.
    If any iteration is present, all of them are (e.g., if B10001A is
    present, so are B10001B, ... , B10001I.)
    """

    tabulation_code = re.match(r'^(B|C)(\d+)[A-Z]?', tables[0]).group(2)

    # 'C' table with no iterations, e.g., C10001
    if 'C' + tabulation_code in tables:
        return 'C' + tabulation_code

    # 'B' table with no iterations, e.g., B10001
    if 'B' + tabulation_code in tables:
        return 'B' + tabulation_code

    # 'C' table with iterations, choosing 'A' iteration, e.g., C10001A
    if 'C' + tabulation_code + 'A' in tables:
        return 'C' + tabulation_code + 'A'

    # 'B' table with iterations, choosing 'A' iteration, e.g., B10001A
    if 'B' + tabulation_code + 'A' in tables:
        return 'B' + tabulation_code + 'A'

    else:
        return ''

def process_fulltext_result(row):
    """ Converts a SQLAlchemy RowProxy to a dictionary.

    params: row - row object returned from a query
    return: dictionary with either profile or table attributes """

    row = dict(row)

    if row['type'] == 'profile':
        result = {
            'type': 'profile',
            'full_geoid': row['full_geoid'],
            'full_name': row['display_name'],
            'sumlevel': row['sumlevel'],
            'sumlevel_name': row['sumlevel_name'] if row['sumlevel_name'] else '',
            'label': 'PROFILE {} {}'.format(row['full_geoid'],row['full_name'])
        }

    elif row['type'] == 'table':
        table_id = choose_table(row['tables'].split())

        result = {
            'type': 'table',
            'table_id': table_id,
            'tabulation_code': row['tabulation_code'],
            'table_name': row['table_title'],
            'simple_table_name': row['simple_table_title'],
            'topics': row['topics'].split(', '),
            'unique_key': row['tabulation_code'],
            'subtables': row['tables'].split(),
            'label': 'TABLE {} {}'.format(row['tabulation_code'],row['simple_table_title'])
        }

    elif row['type'] == 'topic':
        result = {
            'type': 'topic',
            'topic_name': row['topic_name'],
            'url': row['url'],
            'label': 'TOPIC {}'.format(row['topic_name'])
        }

    return result


def perform_full_text_search(db, q, search_type, limit):
    # Support choice of 'search type' as returning table results, profile
    # results, topic results, or all. Only the needed queries will get
    # executed; e.g., for a profile search, the profiles list will be filled
    # but tables and topics will be empty.

    # Build query by replacing apostrophes with spaces, separating words
    # with '&', and adding a wildcard character to support prefix matching.
    q = ' & '.join(q.split())
    q += ':*'

    profiles, tables, topics = [], [], []

    if search_type == 'profile' or search_type == 'all':
        profiles = do_search(db, q, 'profile', limit)

    if search_type == 'table' or search_type == 'all':
        tables = do_search(db, q, 'table', limit)

    if search_type == 'topic' or search_type == 'all':
        topics = do_search(db, q, 'topic', limit)

    # Compute ranking scores of each object that we want to return
    results = []

    for row in profiles + tables + topics:
        results.append((row, compute_score(row)))

    # Sort by second entry (score), descending; the lambda pulls the second
    # element of a tuple.
    results = sorted(results, key=lambda x: x[1], reverse=True)

    # Format of results is a list of tuples, with each tuple being a profile
    # or table followed by its score. The profile or table is then result[0].
    prepared_result = []

    for result in results[:limit]:
        processed = process_fulltext_result(result[0])
        processed['score'] = result[1]
        prepared_result.append(processed)
    return prepared_result
