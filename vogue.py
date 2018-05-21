import csv
import json
import queue
import re
import sqlite3
import threading
import time
import atexit
from pprint import pprint

import requests
from bs4 import BeautifulSoup, SoupStrainer

# VOGUE_CATEGORY_LIST_TEST = ["https://www.vogue.com.tw/jewelry/special"]

VOGUE_CATEGORY_LIST = ["https://www.vogue.com.tw/mobile/beauty/list.asp",
                       "https://www.vogue.com.tw/mobile/culture/list.asp",
                       "https://www.vogue.com.tw/mobile/fashion/list.asp",
                       "https://www.vogue.com.tw/mobile/feature/list.asp",
                       "https://www.vogue.com.tw/mobile/jewelry/list.asp",
                       "https://www.vogue.com.tw/mobile/movie/list.asp",
                       "https://www.vogue.com.tw/mobile/SpecialCollection/list.asp",
                       "https://www.vogue.com.tw/mobile/vogue_talk/list.asp"]
VOGUE_CATEGORY_JSON = "vogue_category.json"
VOGUE_ARTICLES_CSV = "vogue_articles.csv"
THREADLIMIT = 10


def execute_db(fname, sql_cmd, articles):
    try:
        con = sqlite3.connect(fname)
        con.executemany(sql_cmd, articles)
        con.commit()
        con.close()
    except sqlite3.Error as e:
        print("【ERROR】 Database error: %s" % e)
    except Exception as e:
        print("【ERROR】 Exception in _query: %s" % e)


def select_db(fname, sql_cmd):
    conn = sqlite3.connect(fname)
    c = conn.cursor()
    c.execute(sql_cmd)
    rows = c.fetchall()
    conn.close()
    return rows


def link_to_url(url):
    page = requests.get(url)
    page.encoding = 'utf-8'
    if page.status_code != 200:
        return None
    else:
        this_soup = BeautifulSoup(page.text, 'html5lib')
        return this_soup


def link_to_url_part(url):
    page = requests.get(url)
    page.encoding = 'utf-8'
    if page.status_code != 200:
        return None
    else:
        strainer = SoupStrainer("header", "title")
        this_soup = BeautifulSoup(page.text, 'html.parser', parse_only=strainer)
        return this_soup


def encode_latin_to_utf(title_str):
    print(title_str)
    try:
        decode_title = title_str.encode('latin-1').decode('utf-8')
        return decode_title
    except UnicodeDecodeError:
        print(UnicodeDecodeError)
        return None


def open_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except ValueError:
                print('empty json')
    except FileNotFoundError:
        print(file_path + " not found")
        return None


def write_json(file_path, content):
    with open(file_path, 'w', encoding='UTF-8') as f:
        json.dump(content, f, indent=2, sort_keys=True, ensure_ascii=False)


def get_article_count(article_dom, article_index=0):
    article = article_dom.find_all("div", "list-wrapper")[article_index]
    if article:
        article_title_url = article.find("div", "text-area").a['href']
        try:
            article_count = article_title_url.split("-")[1].replace(".html", "")
            return article_count
        except IndexError:
            article_index += 1
            if article_index == 10:
                return None
            else:
                return get_article_count(article_dom, article_index)


def find_category_each(index):
    category_lists = []
    for categoryUrl in VOGUE_CATEGORY_LIST:
        index += 1
        # for categoryUrl in VOGUE_CATEGORY_LIST_TEST:
        vogue_soup = link_to_url(categoryUrl)
        print("%d , handling url %s" % (index, categoryUrl))
        article_count = get_article_count(vogue_soup)
        categoryUrlSplit = categoryUrl.split("/")
        if len(categoryUrlSplit) > 4:
            category_name = categoryUrlSplit[4]
        else:
            category_name = categoryUrlSplit[3]

        if (article_count):
            category_list = dict()
            category_list['name'] = category_name
            category_list['url'] = categoryUrl.replace("list.asp", "")
            category_list['count'] = article_count
            category_lists.append(category_list)
        else:
            print(categoryUrl + ' failed')
    write_json(VOGUE_CATEGORY_JSON, category_lists)
    return category_lists


class voguePage:
    def __init__(self, url):
        self.url = url

    def do(self):
        do_crawler(self.url)


def do_crawler(cralwer_url):
    global total_count
    global db_current_index
    global article_result

    total_count += 1
    if total_count > 0 and total_count % 1000 == 0:
        trigger_save_db_csv()
    exec_time = time.time() - start_time
    print("(%5s) %s - %5.4f - %5.4f" % (
        str(total_count), cralwer_url, float(exec_time), float(exec_time / total_count)))
    article_request = link_to_url_part(cralwer_url)
    if not article_request:
        # current_index -= 1
        return None
    article_header = article_request
    if not article_header:
        # current_index -= 1
        return None
    try:
        article_author_em = article_header.find("time", "publishedTime").find_all("span")[1].find("em")
    except AttributeError:
        print('AttributeError')
        print(cralwer_url)
        return None

    if article_author_em:
        article_author_text = article_author_em.text
    else:
        article_author_text = "null"
    if len(article_author_text.split(",")) > 1:
        article_author = article_author_text.split(",")
    elif len(article_author_text.split("、")) > 1:
        article_author = article_author_text.split("、")
    else:
        article_author = [article_author_text]
    for author in article_author:
        author = author.strip()
        if not author:
            continue
        article = dict()
        article_title = article_header.find(re.compile('h[1-6]')).text.strip().replace('"', '')
        article_time = article_header.find("time", "publishedTime")["datetime"].split("T")[0]
        article["title"] = article_title
        article["author"] = author.strip()
        article["time"] = article_time
        article["url"] = cralwer_url
        article["category"] = category['name']
        article_result.append(article)
    return None


def start_crawler(*args):
    que = args[0]
    while que.qsize() > 0:
        job = que.get()
        job.do()


def exit_handler():
    # save_db_csv()
    print('My application is ending!')


def trigger_save_db_csv():
    lock = threading.Lock()
    lock.acquire()
    save_db_csv()
    lock.release()


def save_db_csv():
    global article_result
    global db_name
    if not len(article_result) > 0:
        print("article_result empty")
        return None

    print('save_db_csv')
    try:
        # cmd = 'INSERT INTO vouge_articles (title, author, create_date, link, category) VALUES ("%s", "%s", "%s", "%s", "%s")' % (
        # article["title"], article["author"], article["time"], article["url"], article["category"])
        cmd = "insert into vouge_articles(title, author, create_date, link, category) values (:title, :author, :time, :url, :category)"
        execute_db(db_name, cmd, article_result)
        article_result = []
    except:
        pass

    # with open(VOGUE_ARTICLES_CSV, 'w', encoding='utf-8', newline='') as f:
    #     writer = csv.writer(f, delimiter=',')
    #     writer.writerow(('標題', '作者', '日期', '網址'))
    #     for article in article_result:
    #         writer.writerow(article.values())


def get_articles():
    global db_name
    sql = "SELECT * FROM vouge_articles WHERE (author LIKE \"%Minnie%\" OR author LIKE \"%minnie%\") OR (author = \"Sun\" OR author = \"sun\" OR author = \"Fairy Sun\" OR author = \"Sun Fairy\"  ) GROUP BY title ORDER BY create_date DESC"
    rows = select_db(db_name, sql)
    print(len(rows))
    with open('minnie_vogue.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(('Key', 'title', 'author', 'create_date', 'link', 'category'))
        for row in rows:
            print(row)
            writer.writerow((column for column in row))


if __name__ == '__main__':
    db_name = 'vogue.db'

    atexit.register(exit_handler)
    start_time = time.time()
    category_lists = open_json(VOGUE_CATEGORY_JSON)

    if not category_lists or len(category_lists) == 0:
        index = 0
        category_lists = find_category_each(index)

    article_total = 1
    for category in category_lists:
        category_count = int(category["count"])
        if int(category_count) > article_total:
            article_total = category_count

    db_current_index = 0
    cmd = 'SELECT * FROM vouge_articles order by key DESC LIMIT 1'
    for row in select_db(db_name, cmd):
        db_url = row[4]
        db_current_index = int(db_url.split("-")[1].replace(".html", "")) - 1

    article_result = []
    total_count = 0
    que = queue.Queue(0)

    current_index = int(article_total)
    if db_current_index > 0:
        current_index = db_current_index

    # current_index = 39493
    while current_index > 0:
        # https://www.vogue.com.tw/mobile/beauty/content-39982.html
        article_mobile_url = "https://www.vogue.com.tw/mobile/beauty/content-%s.html" % (current_index)
        que.put(voguePage(article_mobile_url))
        current_index -= 1

    workerList = []
    for i in range(THREADLIMIT):
        worker = threading.Thread(target=start_crawler, args=(que,))
        worker.start()
        workerList.append(worker)

    for i in range(THREADLIMIT):
        workerList[i].join()

print("總共有 %d 篇文章" % (total_count))

exit(0)
