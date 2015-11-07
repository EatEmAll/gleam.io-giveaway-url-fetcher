# Gleam-giveaway-url-fetcher
## Description
This is a tool for collection of giveaway links on the **gleam.io** platform from twitter and reddit (using tweepy and 
praw modules respectively) as well as for filtering out giveaways that have ended or are unavailable 
(using Selenium webdriver).

All links are saved in urls.json.

## Requirements
- Python 3
- Modules: [tweepy](https://github.com/tweepy/tweepy), [praw](https://github.com/praw-dev/praw), 
[Selenium Python Bindings](http://selenium-python.readthedocs.org/), 
[BeautifulSoup](http://www.crummy.com/software/BeautifulSoup/)
- [phantomjs](http://phantomjs.org/) Note: If you're usiing windows you'll need to append the executable's path to
 the value of Path in Environment Variables.
- Twitter API access (for tweepy). You'll need to generate your own Consumer Key and Consumer Secret and add them to
the config.ini file.

## Usage
### available operations:
- **search** - search for new giveaways using the selected source.

> gleam_url_fetcher -o search -s ***twitter\reddit*** -kw ***gleam giveaway*** -mt ***number_of_tweets*** -mr 
***number_of_reddit_submissions*** -tc ***number_of_threads***

- **utd** - update title and description for each link.

> gleam_url_fetcher -o utd -tc ***number_of_threads***

- **ukeys** - remove duplicate links.

> gleam_url_fetcher -o ukeys -tc ***number_of_threads***

- **ru** - remove unavailable giveaways.

> gleam_url_fetcher -o ru -tc ***number_of_threads***

- **print** - print all giveaways in database.

> gleam_url_fetcher -o print
