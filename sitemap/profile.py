from jinja2 import Environment, FileSystemLoader
import psycopg2
import re


def build_all_sitemaps():
    ''' Builds sitemap XML files for all summary levels. Each XML file contains pages for one
    summary level, with a maximum of 50,000 URLs.

    params: none
    return: none

    '''

    levels_urls = build_all_page_lists()

    for level in levels_urls:
        num_urls = len(levels_urls[level])

        # If there are <= 50k URLs, write them immediately
        if num_urls <= 50000:
            fname = 'profiles/sitemap_' + level + '.xml'
            f = open(fname, 'w')

            f.write(build_sitemap(levels_urls[level]))

            print 'Wrote sitemap to file %s' % (fname)

            f.close()

        # Otherwise, split up the URLs into groups of 50,000
        else:
            num_files = num_urls / 50000 + 1

            for i in range(num_files):
                fname = 'profiles/sitemap_' + level + '_' + str(i + 1) + '.xml'
                f = open(fname, 'w')

                for url in levels_urls[level][i * 50000 : (i + 1) * 50000]:
                    # Python allows list indexing out of bounds without complaint, 
                    # i.e., if L = [1, 2, 3], then L[2:4] just gives [3]

                    f.write("%s\n" % url)

                print 'Wrote sitemap to file %s' % (fname)

                f.close()


def build_sitemap(page_data):
    ''' Builds sitemap from template in sitemap.xml using data provided
    in page_data. 

    params: page_data = list of page URLs
    returns: XML template with the page URLs

    '''

    env = Environment(loader = FileSystemLoader('.'))
    template = env.get_template('sitemap.xml')
    
    return template.render(pages = page_data)


def build_all_page_lists():
    ''' Builds a URL/page list for all sumlevels.

    params: none
    return: dict of {level:list of URLs for that level}

    '''

    levels = query_all_levels()
    urls = {}

    for level in levels:
        urls[level] = build_one_page_list(level)

    return urls


def build_one_page_list(level):
    ''' Builds a URL/page list for one sumlevel ("level")

    params: level = string of the summary level code (e.g., '040')
    return: list of slugified URLs

    '''

    results = query_one_level(level)
    urls = []

    for result in results:
        urls.append(build_url(result[1], result[2]))

    return urls


def query_all_levels():
    ''' Queries database to get list of all sumlevels 

    params: none
    returns: list of all sumlevels (strings)

    '''

    conn = psycopg2.connect("dbname=census user=census")
    cur = conn.cursor()

    q = "SELECT DISTINCT sumlevel FROM tiger2014.census_name_lookup;"
    cur.execute(q)
    results = cur.fetchall()
    # Format of results is [('000',), ('001',), ...]
    # so we make it into a straight list ['000', '001', ...]

    results_list = [c[0] for c in results]

    return results_list


def query_one_level(level):
    ''' Queries database for one sumlevel ("level") 
    
    params: level = string of the summary level code (e.g., "040")
    return: all results found as a list of tuples 
            (sumlevel, display_name, full_geoid)

    '''

    conn = psycopg2.connect("dbname=census user=census")
    cur = conn.cursor()

    q = "SELECT sumlevel, display_name, full_geoid from tiger2014.census_name_lookup where sumlevel = '%s'" % (level)
    cur.execute(q)
    results = cur.fetchall()

    return results


def build_url(display_name, full_geoid):
    ''' Builds the censusreporter URL out of name and geoid.
    Format: https://censusreporter.org/profiles/full_geoid-display_name/"

    params: display_name = (string) name of the area,
            full_geoid = (string) geoid code for the URL
    return: full URL of the relevant page

    >>> build_url("Indiana", "04000US18")
    "https://censusreporter.org/profiles/04000US18-indiana"

    >>> build_url("Columbus, IN Metro Area", "31000US18020")
    "https://censusreporter.org/profiles/31000US18020-columbus-in-metro-area"

    '''

    new_name = slugify(display_name)
    return "https://censusreporter.org/profiles/" + full_geoid + "-" + new_name + "/"


def slugify(name):
    ''' Slugifies a string by (1) removing non-alphanumeric / space characters,
    (2) converting to lowercase, (3) turning spaces to dashes

    params: name = string to change
    return: slugified string 

    '''

    # Remove non-alphanumeric or non-space characters
    name = re.sub('[^0-9a-zA-Z ]', '', name)

    # Lowercase
    name = name.lower()

    # Spaces to dashes
    return name.replace(' ', '-')


def main():
    build_all_sitemaps()


# Some tests
assert slugify("This is a test") == "this-is-a-test"
assert slugify("more ** complicated-- !!test") == "more--complicated-test"
assert build_url("Indiana", "04000US18") == "https://censusreporter.org/profiles/04000US18-indiana/"
assert build_url("Columbus, IN Metro Area", "31000US18020") == "https://censusreporter.org/profiles/31000US18020-columbus-in-metro-area/"


if __name__ == "__main__":
    main()