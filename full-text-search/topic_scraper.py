from HTMLParser import HTMLParser
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


def get_list_of_topics():
    """ Gets list of topics from Census Reporter website """

    url = "https://censusreporter.org/topics"
    handle = urllib2.urlopen(url)
    html = handle.read()

    parser = TopicsParser()
    parser.feed(html)
    from pprint import pprint; pprint(parser.topics)

    return


if __name__ == "__main__":
    get_list_of_topics()