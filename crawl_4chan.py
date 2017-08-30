#!/usr/bin/env python3
# coding: utf-8

import requests
import records
from time import time, sleep
import re
import html
from tqdm import tqdm

BOARD_LIST = ['b', 'lgbt', 'pol', 'r9k', 'bant', 'i', 'a']

LAST_TIME = 0.0

AUTH_SECRET = "./auth.secret" #content: user:password@hostname[\n]
DB_SIZE_SQL = "./db_size.sql"

# Do not make more than one request per second.
def wait_get(url):
    global LAST_TIME
    while time() - LAST_TIME <= 1.0:
        sleep(0.1) # This is what the API wants, not me..!
    r = requests.get(url)
    LAST_TIME = time()
    return r

# make clear text from 4chan posts
def clear_post(comment):
    comment = re.sub(r'<a href="#p\d+" class="quotelink">&gt;&gt;\d+<\/a>(<br>)*', '', comment)
    comment = re.sub(r'<br>', '\n', comment)
    comment = re.sub(r'<span class="quote">.*?<\/span>', '', comment)
    comment = re.sub(r'<.*?>', '', comment)
    comment = re.sub(r'^&gt;', '', comment, flags=re.MULTILINE)
    comment = re.sub(r'^&gt;\d+', '', comment, flags=re.MULTILINE)
    comment = html.unescape(comment)
    comment = re.sub(r'http\S+', '', comment)
    return '\n'.join([x for x in comment.split("\n") if x != ''])

if __name__ == '__main__':

    it_count = 0
    er_count = 0
    start_time = time()
    while True:

        try:
            it_count += 1
            running_time = (time() - start_time) / 3600
            print("\033[31m[ITERATION {}]\033[37m Running for {:.2f}h with {} errors. [{:.2f} IT/s]".format(it_count, running_time, er_count, running_time / it_count))

            # login database
            db = records.Database("postgresql://{}/cancer-corpus".format(open(AUTH_SECRET, "r").read().split()[0]))

            # iterate over all interesting boards
            for board_name in BOARD_LIST:
                r = wait_get("https://a.4cdn.org/{}/threads.json".format(board_name))
                board = r.json()
                relsize = db.query_file(DB_SIZE_SQL)[0]["total"]
                postcnt = db.query("select count(*) as count from fourchan")[0]["count"]
                print("\033[36m[CRAWLING /{}/]\033[37m database_size:{}; post_count:{}".format(board_name, relsize, postcnt))

                # iterate over all pages (11) in the board
                for page in board:
                    print("Crawling 4chan.org/{}/{}".format(board_name, page["page"]))
                    post_counter = 0

                    # iterate over all threads (15) on the page
                    for thread in tqdm(page["threads"]):
                        r = wait_get("https://a.4cdn.org/{}/thread/{}.json".format(board_name, thread["no"]))
                        try: # this sometimes raises strange JSON errors
                            thread = r.json()
                        except:
                            continue

                        # iterate over all posts (>= 1) in the thread
                        for post in thread["posts"]:
                            if "com" in post and "no" in post:

                                # test if post already existing
                                r = db.query("SELECT count(1) AS out FROM fourchan WHERE post_id = :id", id=post['no'])
                                if r[0]["out"] != 0: continue

                                # process comment
                                comment = clear_post(post["com"])
                                if comment == "": continue
                                post_counter += 1

                                # insert comment into database
                                db.query("INSERT INTO fourchan\
                                          VALUES (:id, :com)",
                                          id=post["no"], com=comment)
                    print("  - Added {} new posts!".format(post_counter))

        except KeyboardInterrupt as e:
            raise e
        except:
            er_count += 1
            continue
