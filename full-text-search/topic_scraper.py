# -*- coding: utf-8 -*-

from HTMLParser import HTMLParser
import psycopg2
import re
import urllib2


class HTMLStripper(HTMLParser):
    """ Stripper for HTML tags; simply stores data in self.data. """

    def __init__(self):
        self.reset()
        self.data = []

    def handle_data(self, data):
        """ Append any non-HTML data to our data list.

        Data, by definition, is anything that is not an HTML tag. This is
        exactly what we are interested in.
        """

        self.data.append(data)

    def get_data(self):
        """ Return data as a string. """

        return ''.join(self.data)


class TopicsParser(HTMLParser):
    """ Parser for the main topics page.

    Attributes:
        in_dt_tag: Flag for whether or not the parser is inside a <dt> tag.
        topic_buffer: Buffer to store a single topic name and page URL
        base_url: Census Reporter URL
        topics: List of topic page dictionaries, each containing
                'name' and 'link'

    We always encounter, in order, a <dt> tag, an <a> tag, and the topic name.
    We can take advantage of this data knowledge in order to build a buffer
    that stores a URL, then a topic, which we can then append to our master
    list of topics.
    """

    def __init__(self):
        HTMLParser.__init__(self)
        self.in_dt_tag = False
        self.topic_buffer = {'name': '', 'url': ''}
        self.base_url = "https://censusreporter.org"
        self.topics = []

    def handle_starttag(self, tag, attrs):
        """ Handle <dt> and <a> tags.

        If we see a <dt> tag, set the flag appropriately. Census Reporter's
        topic page does not nest these tags. If we see a link, build and store
        the appropriate URL.
        """

        if tag == 'dt':
            self.in_dt_tag = True

        if self.in_dt_tag and tag == 'a':
            topic_url = self.base_url + attrs[0][1]
            self.topic_buffer['url'] = topic_url

    def handle_endtag(self, tag):
        """ Handle </dt> tag by resetting flag. """

        if tag == 'dt':
            self.in_dt_tag = False

    def handle_data(self, data):
        """ Find data found in the <dt> tags, which are topic names.

        Note that we use topic_buffer.copy() to prevent pointer-like
        behavior, and create new dictionaries when appending them.
        """

        if self.in_dt_tag:
            self.topic_buffer['name'] = data
            self.topics.append(self.topic_buffer.copy())


class TopicPageParser(HTMLParser):
    """ Parser for an individual topic page.

    Attributes:
        in_body: Counter for whether or not parser is in main section of page.
                 This functions more or less like a stack, where we increment
                 it if we reach a relevant <section> tag, and decrement if we
                 reach a </section> tag. If it's greater than 0, then we are
                 in the main body of the page.
        text: List to store all the relevant text snippets on the page
        tables: Dictionary of table code : annotations pairs, where the table
                code represents a table on the page and the annotations are 
                the annotations next to it
        table_codes: List of all table codes.

    The main page content is stored in a <section id='topic-overview'> tag
    or a <section id='topic-elsewhere'> tag. We take advantage of this to find
    the relevant information on the page (and ignore things like scripts or
    footers).
    """

    def __init__(self, html):
        HTMLParser.__init__(self)
        self.in_body = 0
        self.text = []
        self.tables = self.find_all_tables(html)
        self.table_codes = self.tables.keys()

    def handle_starttag(self, tag, attrs):
        """ Handle start tag by detecting main section of page. """

        if tag == 'section' and (('id', 'topic-overview') in attrs
                                 or ('id', 'topic-elsewhere') in attrs):
            self.in_body += 1

    def handle_endtag(self, tag):
        """ Handle end tag by detecting end of main section of page. """

        if tag == 'section' and self.in_body:
            self.in_body -= 1

    def handle_data(self, data):
        """ Add data to the text buffer. """

        if self.in_body:
            # Get rid of non-alphanumeric and non-space / dash / slash
            # characters, plus newline characters to avoid concatenating lines.
            # Then replace the newlines, slashes, and dashes with spaces.
            # This is kind of crude, but ultimately all we care about is a
            # long document of words.
            data = re.sub('[^A-Za-z0-9\-/\n ]', '', data)
            data = re.sub('[\n/-]', ' ', data)
            self.text.append(data.strip())


    def find_all_tables(self, text):
        """ Find all table codes in text using regex 

        Table codes are formatted as [B/C]##### with an optional race iteration
        (character A - H) or a Puerto Rico tag (string 'PR' at the end). 

        Occasionally, there are annotations on the topic pages following the
        table code. These are one of the following characters: 
            ‡ - collapsed version exists; 'collapsed'
            † - has racial iterations; 'iterations'
            § - has Puerto Rico version; 'puerto_rico'
            ª - no core table, only iterations; 'no_core'
        """

        # Strip all the HTML tags
        stripper = HTMLStripper()
        stripper.feed(text.decode('utf-8'))
        text = stripper.get_data()

        # Find table codes
        exp = '([BC]\d{5}[A-H]?P?R?)'
        all_tables = re.finditer(exp, text)

        # Prepare to find all tables on page
        tables_on_page = {}
        annotations = { u'‡' : 'collapsed', u'†' : 'iterations',
                        u'§' : 'puerto_rico', u'ª' : 'no_core' }

        for match in all_tables:
            code = match.group()

            # Add code to tables_on_page if it's not there
            if code not in tables_on_page.keys():
                tables_on_page[code] = []

            # Search for annotations in the four characters after the
            # table code (since there are a maximum of four annotations)
            end_pos = match.end()
            potential_annotations = match.string[end_pos : end_pos + 4]
            actual_annotations = []

            for char in annotations.keys():
                if char in potential_annotations:
                    actual_annotations.append(char)

            # Update tables_on_page with the new annotations, no duplicates
            tables_on_page[code] = list(set(tables_on_page[code] 
                                            + actual_annotations))

        return tables_on_page


class GlossaryParser(HTMLParser):
    """ Parser for the glossary page, censusreporter.org/glossary.

    Attributes:
        in_body: Flag for whether or not parser is in main section of page.
                 We don't have to keep a counter as before, because the
                 glossary page is not structured in a way that there are
                 nested tags.
        in_term_name: Flag for the whether or not the parser is inside a term
                      name. This allows it to keep a separate list of terms.
        terms: List of terms on page.
        text: List of text on page.

    Once again, we use the fact that the body of the page is enclosed in a tag
    <article id='glossary'>. Similarly, term names are always enclosed in <dt>
    tags within the body. Upon encountering a term, it is added to the list of
    terms. Upon encountering any text (including term names), it is added to
    the list of all text.

    """

    def __init__(self):
        HTMLParser.__init__(self)
        self.in_body = False
        self.in_term_name = False
        self.terms = []
        self.text = []

    def handle_starttag(self, tag, attrs):
        """ Handle start tag by detecting body and term names.

        We need to know when we're in the body of the page (again, to avoid
        things like scripts or footers) and when we're in a term name (so that
        those can be documented with higher priority).
        """

        if tag == 'article' and ('id', 'glossary') in attrs:
            self.in_body = True

        if tag == 'dt':
            self.in_term_name = True

    def handle_endtag(self, tag):
        """ Handle end tag by detecting end of body and term names. """

        if tag == 'article' and self.in_body:
            self.in_body = False

        if tag == 'dt':
            self.in_term_name = False

    def handle_data(self, data):
        """ Handle body text and term names on page.

        Add term names found to the list of terms we maintain. Add all text
        found to the list of text.
        """

        if self.in_body:
            data = re.sub('[^A-Za-z0-9\-/\n ]', '', data)
            data = re.sub('[\n/-]', ' ', data)
            self.text.append(data.strip())

        if self.in_term_name:
            self.terms.append(data)


def get_list_of_topics():
    """ Gets and returns list of topics from Census Reporter website.

    Topics are formatted as [{name: topic1, url: url1},
                             {name: topic2, url: url2}, ...]
    """

    url = "https://censusreporter.org/topics"
    handle = urllib2.urlopen(url)
    html = handle.read()
    handle.close()

    parser = TopicsParser()
    parser.feed(html)

    return parser.topics


def scrape_topic_page(name, url):
    """ Scrapes a single topic page to get description and list of tables. """

    handle = urllib2.urlopen(url)
    html = handle.read()
    handle.close()

    parser = TopicPageParser(html)
    parser.feed(html)

    text = ' '.join(parser.text)

    return text, parser.tables, parser.table_codes


def scrape_glossary_page():
    """ Scrapes and returns terms and text found on the glossary page. """

    url = "https://censusreporter.org/glossary"
    handle = urllib2.urlopen(url)
    html = handle.read()
    handle.close()

    parser = GlossaryParser()
    parser.feed(html)

    return {'text': ' '.join(parser.text), 'terms': ' '.join(parser.terms) }


def remove_old_topics():
    """" Removes old topics entries from search_metadata. """

    # Connect to database
    connection = psycopg2.connect("dbname=census user=census")
    cur = connection.cursor()

    # Remove old entries
    q = "DELETE FROM search_metadata WHERE type = 'topic';"

    cur.execute(q)
    print cur.statusmessage

    connection.commit()
    cur.close()
    connection.close()

    return


def add_topics_to_table(topics_data):
    """ Adds topics data into the search_metadata table.

    Requires that the format be a list of dictionaries, i.e.,
        [{name: 'topic1', url: 'url1', table_codes: [tables_in_topic1], 
          text: '...', tables: {not relevant}},
         {name: 'topic2', url: 'url2', table_codes: [tables_in_topic2], 
          text: '...', tables: {not relevant}},
         ... ]
    """

    # Connect to database
    connection = psycopg2.connect("dbname=census user=census")
    cur = connection.cursor()

    for topic in topics_data:
        # Format each "text" entry properly, i.e., &-delimited. We replace spaces
        # with &s, but trim whitespace because there may be multiple sequential
        # spaces.
        topic['text'] = re.sub('\s+', ' ', topic['text'].strip())
        topic['text'] = topic['text'].replace(' ', ' & ')

        # Update search_metadata accordingly. We set text1 to the topic name,
        # text2 to the list of tables, text3 to the URL, and text4 through
        # text6 to NULL. The document is made out of the title (first priority)
        # and the words scraped (third priority)

        q = """INSERT INTO search_metadata
               (text1, text2, text3, text4, text5, text6,
                    type, document)
               VALUES ('{0}', '{1}', '{2}', NULL, NULL, NULL, 'topic',
                    setweight(to_tsvector('{0}'), 'A') ||
                    setweight(to_tsvector('{3}'), 'C'));""".format(
               topic['name'], ' '.join(topic['table_codes']), topic['url'], topic['text'])

        cur.execute(q)
        print cur.statusmessage

    connection.commit()
    cur.close()
    connection.close()

    return


def add_glossary_to_table(glossary):
    """ Add glossary data to search_metadata table.

    Requires that it be formatted as { terms: [term1, term2, ...], text: '...'}
    """

    # Connect to database
    connection = psycopg2.connect("dbname=census user=census")
    cur = connection.cursor()

    # Format text properly, i.e., &-delimited and without multiple spaces
    glossary['text'] = re.sub('\s+', ' ', glossary['text'].strip())
    glossary['text'] = glossary['text'].replace(' ', ' & ')

    glossary['terms'] = re.sub('\s+', ' ', glossary['terms'].strip())
    glossary['terms'] = glossary['terms'].replace(' ', ' & ')

    # Update search_metadata. Set text1 to 'glossary', text2 to the terms,
    # text3 to the URL, and text4 through text6 to NULL. Document is made out
    # of the terms (first priority) and text (third priority)

    q = """INSERT INTO search_metadata
           (text1, text2, text3, text4, text5, text6, type, document)
           VALUES ('Glossary', '{0}', 'https://censusreporter.org/glossary',
                   NULL, NULL, NULL, 'topic',
                   setweight(to_tsvector('{0}'), 'A') ||
                   setweight(to_tsvector('{1}'), 'C'));""".format(
           glossary['terms'], glossary['text'])

    cur.execute(q)
    print cur.statusmessage

    connection.commit()
    cur.close()
    connection.close()

    return


if __name__ == "__main__":
    topics = get_list_of_topics()
    print "Obtained list of topics"

    for topic in topics:
        # Update topics dictionary with the text and tables that are
        # scraped from the topic page.
        topic['text'], topic['tables'], topic['table_codes'] = scrape_topic_page(**topic)
        print "Finished scraping topic page '{0}'".format(topic['name'])

    glossary = scrape_glossary_page()
    print "Finished sraping glossary page"

    remove_old_topics()
    print "Removed old topics entries from search_metadata."
    add_topics_to_table(topics)
    add_glossary_to_table(glossary)
    print "Added new topics entries to search_metadata."