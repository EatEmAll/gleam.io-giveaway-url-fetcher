import json
import threading
from queue import Queue
import argparse
import sys
import re
from enum import Enum
from collections import defaultdict
import random
from configparser import ConfigParser
import socket
import http.client
import time
from urllib.error import URLError

import praw
import requests
from requests.exceptions import BaseHTTPError, RequestException
import tweepy
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

requests.packages.urllib3.disable_warnings()  # disable requests warnings

# Files
XPATH_FILE = 'xpaths.json'
URL_FILE = 'urls.json'
UNRESOLVED_URLS_FILE = 'unresolved_urls.json'
CONFIG_FILE = 'config.ini'

# Globals:
DATA_SOURCE = Enum('data_source', 'reddit twitter')
GLEAM_DOMAIN = 'https://gleam.io/'
CONFIG_DICT = {}  # holds any variables declared in config.ini
HEADERS = {}
DC_PHANTOMJS = dict(DesiredCapabilities.PHANTOMJS)  # Desired capabilities for phantomjs with modified user agent
BROWSER_LOCK = threading.Lock()  # Lock for browser instance creation
SAVE_LOCK = threading.Lock()  # Lock for writing into files
URL_LOCK = threading.Lock()  # Lock for modification of URL_DATA and UNRESOLVED_URLS
REDDIT_Q = Queue()
TWITTER_Q = Queue()
URL_DATA = defaultdict(lambda: defaultdict(lambda: None))
XPATH_DICT = {}
UNRESOLVED_URLS = set()
BROWSERS = []
TOTAL_R_JOBS = 0  # total reddit links to be processed
TOTAL_T_JOBS = 0  # total twitter links to be processed
GIVEAWAY_COUNTER = 0  # total new giveaways discovered

# generate random values for waiting time
t_min = lambda: random.uniform(0.05, 0.15)
t_long = lambda: random.uniform(5, 7)


def load_from_json(file_name):
    with open(file_name, 'r') as f:
        data = json.load(f)
        return data


def save_json_data(file_name, data):
    try:
        with SAVE_LOCK:
            with URL_LOCK:
                with open(file_name, 'w') as f:
                    json.dump(data, f)
    except PermissionError:
        time.sleep(t_min())
        save_json_data(file_name, data)


def fetch_reddit_submissions():
    """Use the praw module to extract links form submissions according to the chosen parameters:
    subreddit, max_submissions, s_flair_text, s_domain (s_flair_text, s_domain are submission properties)
    are all defined in config.ini. The value of max_submissions can be overridden by arg -mr."""
    global TOTAL_R_JOBS
    TOTAL_R_JOBS = 0
    r = praw.Reddit(user_agent=HEADERS['User-Agent'])
    subreddit = r.get_subreddit(CONFIG_DICT['reddit_config']['subreddit'])
    submissions = subreddit.get_hot(limit=CONFIG_DICT['reddit_config']['max_submissions'])
    for submission in submissions:
        if (CONFIG_DICT['reddit_config']['s_flair_text'] in str(submission.link_flair_text).lower()) or \
                (CONFIG_DICT['reddit_config']['s_domain'] in str(submission.domain).lower()):
            if not any(submission.url in urls for urls in (URL_DATA.keys(), UNRESOLVED_URLS)):
                REDDIT_Q.put(submission)
                TOTAL_R_JOBS += 1


def fetch_twitter_links():
    """Generate tweepy_api and search for tweets according to the chosen parameters:
    keyword (the query to search for), tweets_per_qry (keep this at 100), max_tweets are defined in config.ini.
    The value of max_tweets can be overridden by arg -mt."""
    global TOTAL_T_JOBS
    TOTAL_T_JOBS = 0
    # Tweepy authentication
    tweepy_api = get_tweepy_api()
    for keyword in CONFIG_DICT['twitter_config']['keyword']:
        tweet_count = 0
        max_id = -1
        tweets_per_qry = CONFIG_DICT['twitter_config']['tweets_per_qry']
        # Fetch tweets
        while tweet_count < CONFIG_DICT['twitter_config']['max_tweets']:
            try:
                if max_id <= 0:
                    new_tweets = tweepy_api.search(q=keyword, count=tweets_per_qry)
                else:
                    new_tweets = tweepy_api.search(q=keyword, count=tweets_per_qry, max_id=str(max_id - 1))
                if not new_tweets:
                    break

                for tweet in new_tweets:
                    for url in [url_dict['expanded_url'] for url_dict in tweet.entities['urls']]:
                        if url and not any(url in urls for urls in (URL_DATA.keys(), UNRESOLVED_URLS)):
                            TWITTER_Q.put(url)
                            TOTAL_T_JOBS += 1

                tweet_count += len(new_tweets)
                max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                print("some error:", e)
                break


def get_tweepy_api():
    """Generate tweepy api."""
    auth = tweepy.AppAuthHandler(CONFIG_DICT['tweepy_credentials']['consumer_key'],
                                 CONFIG_DICT['tweepy_credentials']['consumer_secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    if not api:
        print("Can't Authenticate")
        sys.exit(-1)
    return api


def get_url_from_string(text):
    """Return the first url within a string"""
    url_pattern = re.compile(r"(?P<url>https?://[^\s]+)")
    result = re.search(url_pattern, text)
    if result:
        return result.group("url")


def giveaway_available(url, browser, xpath_dict=XPATH_DICT):
    """Search for elements in the given url that indicate that the giveaway is unavailable."""
    if not url.startswith(GLEAM_DOMAIN):
        return False
    if xpath_dict == {}:
        xpath_dict = XPATH_DICT
    try:
        giveaway_ended = not (find_by_xpath(url, browser, xpath_dict['top right'], t=t_long())).text.isdigit()
    except AttributeError:
        giveaway_ended = None
    giveaway_unavailable = find_by_xpath(url, browser, xpath_dict['globe'])
    giveaway_warning = find_by_xpath(url, browser, xpath_dict['warning'])
    page_not_exist = find_by_xpath(url, browser, xpath_dict['not exist'])
    return not any([giveaway_ended, giveaway_unavailable, giveaway_warning, page_not_exist])


def process_giveaways(data_source):  # Thread function
    """Process links from url_queue:
    - For links that are not on the gleam.io domain attempt to extract from them links that are within gleam.io.
    - Check if the giveaway is available. If it is, add it to URL_DATA and save it."""
    global GIVEAWAY_COUNTER
    if data_source == DATA_SOURCE.reddit:
        url_queue = REDDIT_Q
    elif data_source == DATA_SOURCE.twitter:
        url_queue = TWITTER_Q
    else:
        raise TypeError

    with BROWSER_LOCK:
        browser = webdriver.PhantomJS(desired_capabilities=DC_PHANTOMJS)
    BROWSERS.append(browser)
    while not url_queue.empty():
        data = url_queue.get()
        if data_source == DATA_SOURCE.reddit:
            url = data.url
        else:
            url = data
        url = switch_to_gleam_domain(url)
        if url and url not in URL_DATA.keys() and giveaway_available(url, browser):
            if url not in URL_DATA.keys():
                with URL_LOCK:
                    URL_DATA[url]['title'] = get_tag_content(url, property='og:title')
                    URL_DATA[url]['description'] = get_tag_content(url, property='og:description')
                print('new giveaway:', url)
                GIVEAWAY_COUNTER += 1
                save_json_data(URL_FILE, URL_DATA)

        url_queue.task_done()
    browser.quit()


def update_title_description(url_queue):  # Thread function
    """Update title and description for each url in url_queue. This can be used in cases where title and description
    were unavailable when the url was added to URL_DATA."""
    while not url_queue.empty():
        url = url_queue.get()
        with URL_LOCK:
            URL_DATA[url]['title'] = get_tag_content(url, property='og:title')
            URL_DATA[url]['description'] = get_tag_content(url, property='og:description')
        save_json_data(URL_FILE, URL_DATA)
        if URL_DATA[url]['title']:
            print(url, URL_DATA[url]['title'].encode('utf-8'))
        url_queue.task_done()


def update_keys(url_queue):  # Thread function
    """Clean up function for cases in which different links in URL_DATA lead to the same giveaway. Normally this
    shouldn't happen."""
    while not url_queue.empty():
        url = url_queue.get()
        new_url = get_url_response(url).url
        if new_url and not new_url.startswith(GLEAM_DOMAIN):
            new_url = switch_to_gleam_domain(new_url)
        if new_url:
            # if not new_url.startswith(GLEAM_DOMAIN):
            #     pass
            if new_url not in URL_DATA.keys():
                with URL_LOCK:
                    URL_DATA[new_url] = URL_DATA[url]
                    URL_DATA.pop(url)
                print('removed url:', url)
                print('new url added:', new_url)
            elif new_url != url:
                with URL_LOCK:
                    URL_DATA.pop(url)
        else:
            with URL_LOCK:
                URL_DATA.pop(url)
            print('removed url:', url)
        save_json_data(URL_FILE, URL_DATA)
        url_queue.task_done()


def remove_unavailable_giveaways(url_queue):  # Thread function
    """For each url in url_queue check if the giveaway is still available. If not, remove it."""
    with BROWSER_LOCK:
        browser = webdriver.PhantomJS(desired_capabilities=DC_PHANTOMJS)
    BROWSERS.append(browser)
    while not url_queue.empty():
        url = url_queue.get()
        if not giveaway_available(url, browser):
            with URL_LOCK:
                URL_DATA.pop(url)
                if url not in UNRESOLVED_URLS:
                    UNRESOLVED_URLS.add(url)
            save_json_data(URL_FILE, URL_DATA)
            print('removed giveaway:', url)
        url_queue.task_done()
    browser.quit()


def switch_to_gleam_domain(url, recursive=True):
    """For links that are not on the gleam.io domain, attempt to extract from their source links that are on
    gleam.io. recursive - if no suitable link is found allow 1 level of recursion with links that are found in source.
    """
    linkis_domain = 'http://linkis.com/'
    pattern = re.compile(r'^[A-Za-z0-9]{5}/$')

    if not url.startswith(GLEAM_DOMAIN):
        if url in UNRESOLVED_URLS:
            return
        r = get_url_response(url)
        if not r or r.url in UNRESOLVED_URLS:
            return
        soup = BeautifulSoup(r.content, "html.parser")

        get_query = lambda tag, **kwargs: dict(tag=tag, attrs=dict(**kwargs))
        queries = (get_query('a', href=True), get_query('meta', content=True), get_query('iframe', src=True),
                   get_query('title'))

        for query in queries:
            attr = [str(k) for k in query['attrs'].keys()]
            if attr:
                attr = attr[0]
            results = soup.find_all(query['tag'], **query['attrs'])
            for result in results:
                if attr:
                    temp_result = result[attr]
                else:
                    temp_url = get_url_from_string(result.text)
                    if temp_url:
                        temp_r = get_url_response(temp_url)  # request url from text within the 'title' tag
                        if not temp_r:  # url request failed
                            continue
                        temp_result = temp_r.url
                    else:
                        continue

                if temp_result.startswith(GLEAM_DOMAIN):
                    giveaway_id = (temp_result[len(GLEAM_DOMAIN):])[:6]
                    if not pattern.match(giveaway_id) or 'login' in giveaway_id:
                        continue
                    # return request_url_get(temp_result, recursive=True).url  # Switching domain succeeded
                    return get_url_address(temp_result)

                elif not attr:
                    if temp_result.startswith(linkis_domain) and \
                            temp_result[len(linkis_domain):].startswith(GLEAM_DOMAIN[len('https://'):]):
                        # return request_url_get('https://' + temp_result[len(linkis_domain):], recursive=True).url
                        return get_url_address('https://' + temp_result[len(linkis_domain):])
                    elif recursive:  # Allow recursion once
                        temp_result = switch_to_gleam_domain(temp_result, recursive=False)
                        if temp_result:
                            # return request_url_get(temp_result, recursive=True).url
                            return get_url_address(temp_result)

        print("Switching domain failed:", url)
        if url not in UNRESOLVED_URLS:
            with URL_LOCK:
                UNRESOLVED_URLS.add(url)
            save_json_data(UNRESOLVED_URLS_FILE, list(UNRESOLVED_URLS))
    else:
        giveaway_id = (url[len(GLEAM_DOMAIN):])[:6]
        if pattern.match(giveaway_id) and 'login' not in giveaway_id:
            return get_url_response(url, recursive=True).url


def get_url_response(url, recursive=False):
    try:
        return requests.get(url, headers=HEADERS, verify=False)
    except (BaseHTTPError, RequestException, UnicodeError, OSError):
        if recursive:
            return get_url_response(url)


def get_url_address(url):
    try:
        return get_url_response(url, recursive=True).url
    except AttributeError:
        pass


def find_by_xpath(url, browser, xpath, t=t_min()):
    """Attempt to find an element with the given xpath. t - max amount of time to wait before a TimeoutException is
    thrown. In a case of ConnectionResetError keep calling find_by_xpath until successful or a TimeoutException
    is thrown."""
    num_of_tries = 5
    for _ in range(num_of_tries):
        try:
            if browser.current_url != url:
                browser.get(url)
            element = WebDriverWait(browser, t).until(lambda e: e.find_element_by_xpath(xpath))
            return element
        except TimeoutException:
            return
        except (ConnectionResetError, ConnectionRefusedError, URLError):
            time.sleep(t_min())


def get_tag_content(url, tag='meta', **kwargs):
    """Get the value of content in the chosen tag. Used for finding title and description for giveaways."""
    r = get_url_response(url)
    if not r:  # url request failed
        return
    soup = BeautifulSoup(r.content, "html.parser")
    result = soup.find(tag, content=True, **kwargs)
    if result:
        return result['content']


def close_browsers():
    """Close all open browsers."""
    global BROWSERS
    for browser in BROWSERS:  # close all browsers
        try:
            browser.quit()
        except (http.client.BadStatusLine, socket.error):
            pass
    BROWSERS = []


def search_giveaways(data_source):
    """ Search for new giveaway links from the chosen data_source (twitter or reddit)."""
    global TOTAL_R_JOBS
    global TOTAL_T_JOBS
    global GIVEAWAY_COUNTER
    GIVEAWAY_COUNTER = 0

    data_source_name = str(data_source).split('.')[1]
    print('starting url fetcher on', data_source_name)

    if data_source == DATA_SOURCE.reddit:
        fetch_links(fetch_reddit_submissions, CONFIG_DICT['global_config']['search_wait_t'])
        q = REDDIT_Q
    elif data_source == DATA_SOURCE.twitter:
        fetch_links(fetch_twitter_links, CONFIG_DICT['global_config']['search_wait_t'])
        q = TWITTER_Q
    else:
        raise TypeError

    for i in range(CONFIG_DICT['global_config']['thread_count']):
        t = threading.Thread(target=process_giveaways, args=[data_source])
        t.daemon = True
        t.start()

    print_progress(q, data_source=data_source)
    q.join()
    print('discovered {} new giveaways'.format(GIVEAWAY_COUNTER))
    close_browsers()


def fetch_links(func, t_wait):  # Thread function
    t = threading.Thread(target=func)
    t.daemon = True
    t.start()
    time.sleep(t_wait)


def database_operations(thread_func):
    """Maintenance operations for URL_DATA. Supported operations: update_title_description, update_keys,
    remove_unavailable_giveaways"""
    q = Queue()

    if thread_func == update_title_description:
        print('updating title and description')
        for key in URL_DATA.keys():
            if URL_DATA[key]['title'] == ('' or None):
                q.put(key)
    elif thread_func == update_keys:
        print('updating keys')
        for key in URL_DATA.keys():
            q.put(key)
    elif thread_func == remove_unavailable_giveaways:
        print('removing unavailable giveaways')
        for key in URL_DATA.keys():
            q.put(key)
    else:
        print('unsupported function')
        return

    total_jobs = q.qsize()
    for i in range(CONFIG_DICT['global_config']['thread_count']):
        t = threading.Thread(target=thread_func, args=[q])
        t.daemon = True
        t.start()

    print_progress(q, total_jobs=total_jobs)
    q.join()
    if thread_func == remove_unavailable_giveaways:
        close_browsers()


def initialize_globals():
    """Initialize all global variables. Load data from config.ini to CONFIG_DICT, from xpath.json to XPATH_DICT,
    from urls.json to URL_DATA, from unresolved_urls.json to UNRESOLVED_URLS and from args to CONFIG_DICT"""
    class CustomParser(ConfigParser):
        def as_dict(self):  # get all data as a dictionary
            d = dict(self._sections)
            for k in d:
                d[k] = dict(self._defaults, **d[k])
                d[k].pop('__name__', None)
            return d

    global CONFIG_DICT
    global XPATH_DICT
    global UNRESOLVED_URLS
    config_parser = CustomParser()
    config_parser.read(CONFIG_FILE)
    CONFIG_DICT = config_parser.as_dict()
    int_keys = dict(twitter_config=['max_tweets', 'tweets_per_qry'], reddit_config=['max_submissions'],
                    global_config=['thread_count', 'search_wait_t'])  # keys with int values
    for primary_key, secondary_key_list in int_keys.items():
        for secondary_key in secondary_key_list:
            CONFIG_DICT[primary_key][secondary_key] = int(CONFIG_DICT[primary_key][secondary_key])
    keyword = CONFIG_DICT['twitter_config']['keyword']
    CONFIG_DICT['twitter_config']['keyword'] = []
    CONFIG_DICT['twitter_config']['keyword'].append(keyword)

    args_dict = vars(get_args(sys.argv[1:]))
    args_keys = (dict(pkey='twitter_config', skey='max_tweets', akey='maxt'),
                 dict(pkey='twitter_config', skey='keyword', akey='keyword'),
                 dict(pkey='reddit_config', skey='max_submissions', akey='maxr'),
                 dict(pkey='global_config', skey='thread_count', akey='tcount'))
    # overwrite data from config_dict with given arguments
    for item in args_keys:
        akey_value = args_dict[item['akey']]
        if akey_value:
            if type(akey_value) is int and akey_value > 0 or type(akey_value) is list:
                CONFIG_DICT[item['pkey']][item['skey']] = args_dict[item['akey']]

    HEADERS['User-Agent'] = CONFIG_DICT['global_config']['user_agent']
    DC_PHANTOMJS['phantomjs.page.settings.userAgent'] = HEADERS['User-Agent']

    temp_dict = load_from_json(URL_FILE)
    for key in temp_dict.keys():
        URL_DATA[key].update(temp_dict[key])
    XPATH_DICT = load_from_json(XPATH_FILE)
    UNRESOLVED_URLS = set(load_from_json(UNRESOLVED_URLS_FILE))


def get_args(args=None):
    """Parse arguments"""
    source_types = ['twitter', 'reddit']
    operations = ['search', 'utd', 'ukeys', 'ru', 'print']
    parser = argparse.ArgumentParser(description='collector for Gleam giveaway links from twitter and reddit')
    parser.add_argument('-s', '--source', choices=source_types, nargs='+', help='source type: reddit or twitter',
                        default='twitter')
    parser.add_argument('-o', '--operations', choices=operations, nargs='+', required='True', help=
    '''
    available operations:
    search - search for new giveaways using the selected source.
    utd - update title and description for each lin.
    ukeys - remove duplicate links.
    ru - remove unavailable giveaways.
    print - print all giveaways in database.
    ''')
    parser.add_argument('-kw', '--keyword', help='keywords to search for', type=str, nargs='+')
    parser.add_argument('-mt', '--maxt', help='max tweets to search for', type=int)
    parser.add_argument('-mr', '--maxr', help='max reddit submissions to search for', type=int)
    parser.add_argument('-tc', '--tcount', help='number of threads to run', type=int)
    return parser.parse_args(args)


def print_progress(q, data_source=None, total_jobs=None, t=10):  # print progress every t seconds
    if total_jobs == 0:
        print('queue is empty')
        return
    if not (data_source or total_jobs):
        print('One parameter of the following is required: data_source, total_jobs.')
        raise TypeError

    if data_source:
        if data_source == DATA_SOURCE.reddit:
            total_jobs = TOTAL_R_JOBS
        elif data_source == DATA_SOURCE.twitter:
            total_jobs = TOTAL_T_JOBS
        else:
            raise TypeError

    try:
        percentage = round(100 * (total_jobs - q.unfinished_tasks) / total_jobs, 2)
    except ZeroDivisionError:
        percentage = 100
    print('progress: {} % completed'.format(percentage))
    t = threading.Timer(t, print_progress, args=[q, data_source, total_jobs, t])
    t.daemon = True
    if percentage < 100:
        t.start()


def print_giveaways():
    """print url and title for each giveaway in URL_DATA."""
    print('url database:')
    for key, value in URL_DATA.items():
        title = ''
        if value['title']:
            title = value['title'].encode('utf-8')
        print(key, title)
        if not value['title']:
            print(key, value)
    print("Totally", len(URL_DATA), "submissions.\n")
    print("Totally", len(UNRESOLVED_URLS), "unresolved links.")


def run():
    """Main function: Initialize globals, get args, and perform the chosen operations."""
    initialize_globals()
    args_dict = vars(get_args(sys.argv[1:]))
    print('args:', args_dict)

    if 'search' in args_dict['operations']:
        if 'twitter' in args_dict['source']:
            search_giveaways(DATA_SOURCE.twitter)
        if 'reddit' in args_dict['source']:
            search_giveaways(DATA_SOURCE.reddit)

    db_operations = dict(utd=update_title_description, ukeys=update_keys, ru=remove_unavailable_giveaways)
    for key, value in db_operations.items():
        if key in args_dict['operations']:
            database_operations(value)

    if 'print' in args_dict['operations']:
        print_giveaways()


if __name__ == "__main__":
    run()