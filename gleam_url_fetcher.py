
# TODO: Handle warnings: ResourceWarning, DeprecationWarning

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

import praw
import requests
from requests.exceptions import BaseHTTPError
from requests.exceptions import RequestException
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
UNRESOLVED_URLS = []
BROWSERS = []
TOTAL_R_JOBS = None  # total reddit links to be processed
TOTAL_T_JOBS = None  # total twitter links to be processed
GIVEAWAY_COUNTER = None  # total new giveaways discovered

# generate random values for waiting time
t_min = lambda: random.uniform(0.05, 0.15)
t_long = lambda: random.uniform(5, 7)


def load_from_json(file_name):
    with open(file_name, 'r') as f:
        data = json.load(f)
        return data


def save_json_data(file_name, data):
    with SAVE_LOCK:
        with URL_LOCK:
            with open(file_name, 'w') as f:
                json.dump(data, f)


def save_url(url, data_source):
    default_args = dict(account=None, entries_completed=0, total_entries=None)
    with URL_LOCK:
        if data_source == DATA_SOURCE.twitter or data_source == DATA_SOURCE.reddit:
            URL_DATA[url].update(default_args)
        else:
            raise TypeError
    save_json_data(URL_FILE, URL_DATA)


def fetch_reddit_submissions():
    r = praw.Reddit(user_agent=HEADERS['User-Agent'])
    subreddit = r.get_subreddit(CONFIG_DICT['reddit_config']['subreddit'])
    submissions = subreddit.get_hot(limit=CONFIG_DICT['reddit_config']['max_submissions'])
    for submission in submissions:
        if (CONFIG_DICT['reddit_config']['s_flair_text'] in str(submission.link_flair_text).lower()) or \
                (CONFIG_DICT['reddit_config']['s_domain'] in str(submission.domain).lower()):
            if submission.url not in (URL_DATA.keys() or UNRESOLVED_URLS):
                REDDIT_Q.put(submission)


def fetch_twitter_links():
    # Tweepy authentication
    tweepy_api = get_tweepy_api()

    tweet_count = 0
    max_id = -1
    keyword = CONFIG_DICT['twitter_config']['keyword']
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
                url = get_url_from_string(tweet.text)
                if url and url not in (URL_DATA.keys() or UNRESOLVED_URLS):
                    TWITTER_Q.put(url)

            tweet_count += len(new_tweets)
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            print("some error:", e)
            break


def get_tweepy_api():
    auth = tweepy.AppAuthHandler(CONFIG_DICT['tweepy_credentials']['consumer_key'],
                                 CONFIG_DICT['tweepy_credentials']['consumer_secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    if not api:
        print("Can't Authenticate")
        sys.exit(-1)
    return api


def get_url_from_string(text):  # Return the first url within a string
    url_pattern = re.compile(r"(?P<url>https?://[^\s]+)")
    result = re.search(url_pattern, text)
    if result:
        return result.group("url")


def giveaway_available(url, browser, xpath_dict=XPATH_DICT):
    if xpath_dict == {}:
        xpath_dict = XPATH_DICT
    try:
        giveaway_ended = not (find_by_xpath(url, browser, xpath_dict['top right'], t=t_long())).text.isdigit()
    except AttributeError:
        giveaway_ended = None
    giveaway_unavailable = find_by_xpath(url, browser, xpath_dict['globe'])
    giveaway_warning = find_by_xpath(url, browser, xpath_dict['warning'])
    return not any([giveaway_ended, giveaway_unavailable, giveaway_warning])


def process_input(data_source):  # Thread function
    global GIVEAWAY_COUNTER
    url_queue = None
    total_jobs = None
    if data_source == DATA_SOURCE.reddit:
        url_queue = REDDIT_Q
        total_jobs = TOTAL_R_JOBS
    elif data_source == DATA_SOURCE.twitter:
        url_queue = TWITTER_Q
        total_jobs = TOTAL_T_JOBS
    else:
        raise TypeError

    with BROWSER_LOCK:
        browser = webdriver.PhantomJS(desired_capabilities=DC_PHANTOMJS)
    BROWSERS.append(browser)
    while not url_queue.empty():
        data = url_queue.get()
        url = None
        if data_source == DATA_SOURCE.reddit:
            url = data.url
        else:
            url = data

        url = switch_to_gleam_domain_r(url)
        if url and url not in URL_DATA.keys() and giveaway_available(url, browser):
            with URL_LOCK:
                URL_DATA[url]['title'] = get_tag_content(url, property='og:title')
                URL_DATA[url]['description'] = get_tag_content(url, property='og:description')
            print('new giveaway:', url)
            GIVEAWAY_COUNTER += 1
            save_url(url, data_source)

        url_queue.task_done()
    browser.quit()


def update_title_description(url_queue):  # Thread function
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
    gleam_domain = 'https://gleam.io/'
    while not url_queue.empty():
        url = url_queue.get()
        new_url = request_url_get(url).url
        if new_url and not new_url.startswith(gleam_domain):
            new_url = switch_to_gleam_domain_r(new_url)
        if new_url:
            if new_url not in URL_DATA.keys():
                with URL_LOCK:
                    URL_DATA[new_url] = URL_DATA[url]
                    URL_DATA.pop(url)
                print('new url added:', new_url)
            elif new_url != url:
                with URL_LOCK:
                    URL_DATA.pop(url)
        else:
            with URL_LOCK:
                URL_DATA.pop(url)
        save_json_data(URL_FILE, URL_DATA)
        url_queue.task_done()


def remove_unavailable_giveaways(url_queue):  # Thread function
    with BROWSER_LOCK:
        browser = webdriver.PhantomJS(desired_capabilities=DC_PHANTOMJS)
    BROWSERS.append(browser)
    while not url_queue.empty():
        url = url_queue.get()
        if not giveaway_available(url, browser):
            with URL_LOCK:
                URL_DATA.pop(url)
                if url not in UNRESOLVED_URLS:
                    UNRESOLVED_URLS.append(url)
            save_json_data(URL_FILE, URL_DATA)
            print('removed giveaway:', url)
        url_queue.task_done()
    browser.quit()


def switch_to_gleam_domain_r(url, recursive=True):  # Switch to gleam.io domain if needed
    gleam_domain = 'https://gleam.io/'
    linkis_domain = 'http://linkis.com/'

    if not url.startswith(gleam_domain):
        r = request_url_get(url)
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
                temp_result = None
                if attr:
                    temp_result = result[attr]
                else:
                    temp_url = get_url_from_string(result.text)
                    if temp_url:
                        temp_r = request_url_get(temp_url)  # request url from text within the 'title' tag
                        if not temp_r:  # url request failed
                            continue
                        temp_result = temp_r.url
                    else:
                        continue

                if temp_result.startswith(gleam_domain):
                    if temp_result[len(gleam_domain):].startswith('?via='):
                        continue
                    return request_url_get(temp_result).url  # Switching domain succeeded

                elif not attr:
                    if temp_result.startswith(linkis_domain) and \
                            temp_result[len(linkis_domain):].startswith(gleam_domain[len('https://'):]):
                        return request_url_get('https://' + temp_result[len(linkis_domain):]).url
                    elif recursive:  # Allow recursion once
                        temp_result = switch_to_gleam_domain_r(temp_result, recursive=False)
                        if temp_result:
                            return request_url_get(temp_result).url

        print("Switching domain failed:", url)
        if url not in UNRESOLVED_URLS:
            with URL_LOCK:
                UNRESOLVED_URLS.append(url)
            save_json_data(UNRESOLVED_URLS_FILE, UNRESOLVED_URLS)
    else:
        return request_url_get(url).url


def request_url_get(url):
    try:
        return requests.get(url, headers=HEADERS, verify=False)
    except (BaseHTTPError, RequestException, UnicodeError, OSError):
        pass


def find_by_xpath(url, browser, xpath, t=t_min()):
    try:
        if browser.current_url != url:
            browser.get(url)
        element = WebDriverWait(browser, t).until(lambda e: e.find_element_by_xpath(xpath))
        return element
    except TimeoutException:
        pass
    except ConnectionResetError:
        time.sleep(t_min())
        return find_by_xpath(url, browser, xpath, t=t_min())



def get_tag_content(url, tag='meta', **kwargs):
    r = request_url_get(url)
    if not r:  # url request failed
        return
    soup = BeautifulSoup(r.content, "html.parser")
    result = soup.find(tag, content=True, **kwargs)
    if result:
        return result['content']


def print_progress(q, total_jobs, t=10):  # print progress every t seconds
    if total_jobs == 0:
        print('queue is empty')
        return
    percentage = round(100 * (total_jobs - q.unfinished_tasks) / total_jobs, 2)
    print('progress: {} % completed'.format(percentage))
    t = threading.Timer(t, print_progress, args=[q, total_jobs])
    t.daemon = True
    if percentage < 100:
        t.start()


def reset_to_default_args():
    default_args = dict(account=None, entries_completed=0, total_entries=None)
    for key in URL_DATA.keys():
        URL_DATA[key].update(default_args)
    save_json_data(URL_FILE, URL_DATA)


def close_browsers():
    global BROWSERS
    for browser in BROWSERS:  # close all browsers
        try:
            browser.quit()
        except (http.client.BadStatusLine, socket.error):
            pass
    BROWSERS = []


def run_url_fetcher(data_source):
    global TOTAL_R_JOBS
    global TOTAL_T_JOBS
    global GIVEAWAY_COUNTER
    GIVEAWAY_COUNTER = 0
    q = None
    total_jobs = None

    data_source_name = str(data_source).split('.')[1]
    print('starting url fetcher on', data_source_name)

    if data_source == DATA_SOURCE.reddit:
        fetch_reddit_submissions()
        q = REDDIT_Q
        TOTAL_R_JOBS = q.qsize()
    elif data_source == DATA_SOURCE.twitter:
        fetch_twitter_links()
        q = TWITTER_Q
        TOTAL_T_JOBS = q.qsize()
    else:
        raise TypeError
    total_jobs = q.qsize()
    print('total jobs:', total_jobs)

    for i in range(CONFIG_DICT['global_config']['thread_count']):
        t = threading.Thread(target=process_input, args=[data_source])
        t.daemon = True
        t.start()

    print_progress(q, total_jobs)
    q.join()
    print('discovered {} new giveaways'.format(GIVEAWAY_COUNTER))
    close_browsers()


def database_operations(thread_func):
    q = Queue()
    total_jobs = 0

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
    print('total jobs:', total_jobs)
    for i in range(CONFIG_DICT['global_config']['thread_count']):
        t = threading.Thread(target=thread_func, args=[q])
        t.daemon = True
        t.start()

    print_progress(q, total_jobs)
    q.join()
    if thread_func == remove_unavailable_giveaways:
        close_browsers()


def initialize_globals():
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
                    global_config=['thread_count'])  # keys with int values
    for primary_key, secondary_key_list in int_keys.items():
        for secondary_key in secondary_key_list:
            CONFIG_DICT[primary_key][secondary_key] = int(CONFIG_DICT[primary_key][secondary_key])

    args_dict = vars(get_args(sys.argv[1:]))
    args_keys = (dict(pkey='twitter_config', skey='max_tweets', akey='maxt'),
                 dict(pkey='reddit_config', skey='max_submissions', akey='maxr'),
                 dict(pkey='global_config', skey='thread_count', akey='tcount'))
    # overwrite data from config_dict with given arguments
    for item in args_keys:
        if args_dict[item['akey']] and args_dict[item['akey']] > 0:
            CONFIG_DICT[item['pkey']][item['skey']] = args_dict[item['akey']]

    HEADERS['User-Agent'] = CONFIG_DICT['global_config']['user_agent']
    DC_PHANTOMJS['phantomjs.page.settings.userAgent'] = HEADERS['User-Agent']

    temp_dict = load_from_json(URL_FILE)
    for key in temp_dict.keys():
        URL_DATA[key].update(temp_dict[key])
    XPATH_DICT = load_from_json(XPATH_FILE)
    UNRESOLVED_URLS = load_from_json(UNRESOLVED_URLS_FILE)


def get_args(args=None):
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
    parser.add_argument('-mt', '--maxt', help='max tweets to search for', type=int)
    parser.add_argument('-mr', '--maxr', help='max reddit submissions to search for', type=int)
    parser.add_argument('-tc', '--tcount', help='number of threads to run', type=int)
    return parser.parse_args(args)


def print_giveaways():
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
    initialize_globals()
    args_dict = vars(get_args(sys.argv[1:]))
    print('args:', args_dict)

    if 'search' in args_dict['operations']:
        if 'twitter' in args_dict['source']:
            run_url_fetcher(DATA_SOURCE.twitter)
        if 'reddit' in args_dict['source']:
            run_url_fetcher(DATA_SOURCE.reddit)

    db_operations = dict(utd=update_title_description, ukeys=update_keys, ru=remove_unavailable_giveaways)
    for key, value in db_operations.items():
        if key in args_dict['operations']:
            database_operations(value)

    if 'print' in args_dict['operations']:
        print_giveaways()


if __name__ == "__main__":
    run()
