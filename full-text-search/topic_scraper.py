from HTMLParser import HTMLParser
import re
import urllib2


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
        tables: List to store all table IDs found on the page

    The main page content is stored in a <section id='topic-overview'> tag
    or a <section id='topic-elsewhere'> tag. We take advantage of this to find
    the relevant information on the page (and ignore things like scripts or
    footers).
    """

    def __init__(self):
        HTMLParser.__init__(self)
        self.in_body = 0
        self.text = []
        self.tables = []

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

            if self.is_table(data) and data not in self.tables:
                self.tables.append(data)

    def is_table(self, data):
        """ Detects if a given string is a valid table code.

        Table codes are formatted as [B/C]#####. We need the try/except block
        because the string indices may be out of range (in which case the
        object we're dealing with is certainly not a table).
        """

        try:
            if ((data[0] == "B" or data[1] == "C")
            and data[1:6].isdigit()):
                return True

            return False

        except:
            return False


def get_list_of_topics():
    """ Gets and returns list of topics from Census Reporter website.

    Topics are formatted as [{name: topic1, url: url1},
                             {name: topic2, url: url2}, ...]
    """

    url = "https://censusreporter.org/topics"
    handle = urllib2.urlopen(url)
    html = handle.read()

    parser = TopicsParser()
    parser.feed(html)

    return parser.topics


def scrape_topic_page(name, url):
    """ Scrapes a single topic page to get description and list of tables. """

    handle = urllib2.urlopen(url)
    html = handle.read()

    parser = TopicPageParser()
    parser.feed(html)

    text = ' '.join(parser.text)

    return text, parser.tables


if __name__ == "__main__":
    topics = get_list_of_topics()

    for topic in topics:
        print topic
        print scrape_topic_page(**topic)