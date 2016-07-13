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

    # Make population nonzero.
    if not population:
        population = 1

    priority, population = int(priority), int(population)

    # Decrement priority by 5 to map [5, 320] to [0, 315].
    priority -= 5

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

    cur.execute("""SELECT text1 AS display_name, 
                         text2 AS sumlevel,
                         text3 AS sumlevel_name,
                         text4 AS full_geoid,
                         text5 AS population, 
                         text6 AS priority,
                         ts_rank(document, to_tsquery('simple', '{0}')) AS relevance
                   FROM search_metadata
                   WHERE document @@ to_tsquery('simple', '{0}')
                   AND type = 'profile'
                   ORDER BY CAST(text6 as INT), 
                            CAST(text5 as INT) DESC, 
                            relevance DESC
                   LIMIT 20;
                """.format(' & '.join(q.split())))
    
    return cur.fetchall()


def show_results(results):
    """ Print search results' names and scores. """

    # Format of data is a 7-tuple, with priority being the 6th entry,
    # population being the 5th entry, and profile name being the 1st entry.
    data = [(x[0], compute_score(int(x[5]), int(x[4]))) for x in results]

    for datum in data:
        print datum


if __name__ == "__main__":
    formatted_query = ' '.join(sys.argv[1:])
    query_results = get_results(formatted_query)
    show_results(query_results)