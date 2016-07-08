from math import log
import psycopg2
import sys

PRIORITY_MAX = 320.0 - 5
# Offset by 5 since the best priority is 5, not 0.
# Priority ranges from 5 to 320, with the only sumlevel with priority 5
# being country, with one entry: United States
POP_US = 318857056.0 # APPROXIMATE VALUE!


def compute_score(priority, population):
    """Computes a 0 to 1 ranking score.
    
    Function takes priority and population of place as parameters.
    """
    priority -= 5
    return ((1 - priority / PRIORITY_MAX) * 0.8 +
            (1 + log(population / POP_US) / log(POP_US)) * 0.2)


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
               ORDER BY priority, sumlevel, population DESC, relevance DESC;
            """.format((' & ').join(sys.argv[1:])))

data = cur.fetchall()
data = [(x[0], compute_score(x[6], x[5])) for x in data]

for datum in data:
    print datum
