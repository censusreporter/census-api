from math import log
import psycopg2
import sys

# Priority bounds are 5 (nation) to 320 (something small), so the actual
# range is size 315
PRIORITY_RANGE = 320.0 - 5

# Approximate value, because this is listed differently in different places
POP_US = 318857056.0


def compute_score(priority, population):
    """ Computes a ranking score in the range [0, 1].
    
    params: priority - priority score in range [5, 320]
            population - population of place in range [0, POP_US]
    return: score in range [0, 1]
    """

    # Decrement priority by 5 to map [5, 320] to [0, 315].
    priority -= 5

    # Make population nonzero.
    if not population:
        population = 1

    # The function (1 - priority / PRIORITY_RANGE) sends priorities in 
    # [0, 315] -> [0, 1], with 0 -> 1, 315 -> 0. 
    #
    # The function (1 + log(population / POP_US) / log(POP_US)) sends
    # the range of populations [0, POP_US] -> [0, 1] as well. This assumes
    # a maximum population of that of the United States, sending 0 -> 0,
    # POP_US -> 1. 
    #
    # Since we have two separate rankings in [0, 1], we weight them however
    # we want, as long as the weights add to 1. We choose 0.8 for the weight
    # of the priority, which we found most important, and 0.2 for population.


    return ((1 - priority / PRIORITY_RANGE) * 0.8 +
            (1 + log(population / POP_US) / log(POP_US)) * 0.2)


def get_results(q):
    """ Get profile search results based on a search string q."""

    # Connect to database, execute query
    connection = psycopg2.connect("dbname=census user=census")
    cur = connection.cursor()

    cur.execute("""SELECT m.text1 AS display_name, m.text3,
                         m.text2 AS sumlevel,
                         m.text4 AS full_geoid,
                         ts_rank(m.document, to_tsquery('{0}')) AS relevance,
                         c.population AS population, c.priority AS priority
                   FROM search_metadata m
                   JOIN tiger2014.census_name_lookup c
                   ON m.text4 = c.full_geoid
                   WHERE m.document @@ to_tsquery('{0}')
                   AND m.type = 'profile'
                   ORDER BY priority, sumlevel, population DESC, relevance DESC
                   LIMIT 20;
                """.format(' & '.join(q.split())))
    
    return cur.fetchall()


def show_results(results):
    """ Print search results' names and scores. """

    # Format of data is a 7-tuple, with priority and population being 
    # the last two entries, and profile name being the first entry.
    data = [(x[0], compute_score(x[6], x[5])) for x in results]

    for datum in data:
        print datum


if __name__ == "__main__":
    formatted_query = ' '.join(sys.argv[1:])
    query_results = get_results(formatted_query)
    show_results(query_results)