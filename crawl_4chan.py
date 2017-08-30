#!/bin/env python
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

def wait_get(url):
    global LAST_TIME
    while time() - LAST_TIME <= 1.0:
        sleep(0.1)
    r = requests.get(url)
    LAST_TIME = time()
    return r


if __name__ == '__main__':

    db = records.Database("postgresql://{}/cancer-corpus".format(open(AUTH_SECRET, "r").read().split()[0]))

    for board_name in BOARD_LIST:
        r = wait_get("https://a.4cdn.org/{}/threads.json".format(board_name))
        board = r.json()
        relsize = db.query_file(DB_SIZE_SQL)[0]["total"]
        postcnt = db.query("select count(*) as count from fourchan")[0]["count"]
        print("\033[36m[CRAWLING /{}/]\033[37m database_size:{}; post_count:{}".format(board_name, relsize, postcnt))
        for page in board:
            print("Crawling 4chan.org/{}/{}".format(board_name, page["page"]))
            post_counter = 0
            for thread in tqdm(page["threads"]):
                id = thread["no"]

                url = "https://a.4cdn.org/{}/thread/{}.json".format(board_name, id)
                r = wait_get(url)
                try:
                    thread = r.json()
                except:
                    continue
                for post in thread["posts"]:
                    if "com" in post and "no" in post:

                        r = db.query("SELECT count(1) AS out FROM fourchan WHERE post_id = :id", id=post['no'])
                        if r[0]["out"] != 0: continue # post already existing

                        comment = post["com"]
                        comment = re.sub(r'<a href="#p\d+" class="quotelink">&gt;&gt;\d+<\/a>(<br>)*', '', comment)
                        comment = re.sub(r'<br>', '\n', comment)
                        comment = re.sub(r'<span class="quote">.*?<\/span>', '', comment)
                        comment = re.sub(r'<.*?>', '', comment)
                        comment = re.sub(r'^&gt;', '', comment, flags=re.MULTILINE)
                        comment = re.sub(r'^&gt;\d+', '', comment, flags=re.MULTILINE)
                        comment = html.unescape(comment)
                        comment = re.sub(r'http\S+', '', comment)
                        comment = '\n'.join([x for x in comment.split("\n") if x != ''])
                        if comment == "": continue
                        post_counter += 1
                        db.query("INSERT INTO fourchan\
                                  VALUES (:id, :com)",
                                  id=post["no"], com=comment)
            print("  - Added {} new posts!".format(post_counter))
