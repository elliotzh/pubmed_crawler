import urllib3
import datetime
from os import path
import os
import threading
import json
from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self, processes=20, chunk_size=100):
        self._thread_pool = ThreadPool(processes=processes)
        self._chunk_size = chunk_size

        # temp
        self._local_dir = ""
        self._url_func = lambda x: x
        self._target_suffix = ""

        self._process_func = lambda x: x

    def scrape_all(self, local_dir, target_names, url_func, early_stop_func=None, target_suffix="html"):
        self._url_func = url_func
        self._local_dir = local_dir
        self._target_suffix = target_suffix

        if not path.isdir(self._local_dir):
            os.makedirs(self._local_dir)

        max_i = -1
        if early_stop_func is not None:
            self.scrape_one(target_names[0])
            with open(self.get_target_path(target_names[0]), "r", encoding="utf-8") as first_page_file:
                max_i = early_stop_func(first_page_file.read())
                first_page_file.close()
            assert max_i <= 1000

        for i, _ in enumerate(self._thread_pool.imap(self.scrape_one, target_names[:max_i], chunksize=self._chunk_size)):
            if divmod(i, 1000)[1] == 0:
                print("#{}: {}".format(i, datetime.datetime.now()))

    def get_target_path(self, target_name):
        return path.join(self._local_dir, "{}.{}".format(target_name, self._target_suffix))

    def scrape_one(self, target_name):
        if path.isfile(self.get_target_path(target_name)):
            return target_name

        url_pool = urllib3.PoolManager()
        target_url = self._url_func(target_name)
        if target_url is None:
            print("Can't find source link for pubmed doc id {}".format(target_name))
            return target_name

        try:
            r = url_pool.request("GET", target_url)
        except urllib3.exceptions.MaxRetryError:
            print("can't access pubmed doc {}".format(target_name))
            return target_name
        with open(self.get_target_path(target_name), "wb") as html_file:
            html_file.write(r.data)
            html_file.close()
        return target_name

    def process_one(self, target_name):
        input_path = self._url_func(target_name)
        output_path = path.join(self._local_dir, "{}.{}".format(target_name, self._target_suffix))
        if not path.isfile(input_path):
            return
        with open(input_path, "r", encoding="utf-8") as infile:
            try:
                output = self._process_func(infile.read())
            except:
                infile.close()
                print("Met error in parsing pubmed doc {}.".format(target_name))
                return
            infile.close()

            if output is not None:
                with open(output_path, "w", encoding="utf-8") as outfile:
                    json.dump(output, outfile, indent=2)
                    outfile.close()

    def process_all(self, input_func, output_dir, target_names, process_func, target_suffix="json"):
        self._process_func = process_func
        self._url_func = input_func
        self._local_dir = output_dir
        self._target_suffix = target_suffix

        if not path.isdir(self._local_dir):
            os.makedirs(self._local_dir)

        for i, _ in enumerate(
                self._thread_pool.imap(self.process_one, target_names, chunksize=self._chunk_size)):
            if divmod(i, 1000)[1] == 0:
                print("#{}: {}".format(i, datetime.datetime.now()))


def get_max_page(content):
    soup = BeautifulSoup(content, features="html.parser")
    amount = soup.find("div", {"class": "results-amount"})
    item_count = int(amount.span.text.replace(',', ''))
    return divmod(item_count, 10)[0] + 1


def get_source_link(i, base_dir, journal_name):
    with open("{}\\{}.html".format(base_dir, i), "r", encoding="utf-8") as pubmed_file:
        content = pubmed_file.read()
        if content.find("full-text-links-list") is -1:
            return None
        pubmed_file.close()
        soup = BeautifulSoup(content, features="html.parser")
        link_div = soup.find("div", {"class": "full-text-links-list"})
        if link_div is None:
            return None
        origin_url = link_div.a["href"]
        if journal_name == "Nature":
            nature_id = origin_url.split("nature")[-1]
            return "https://www.nature.com/articles/nature{}".format(nature_id)
        elif journal_name in ["Nature Methods", "Nature Communications", "Nature Biotechnology"]:
            nature_id = origin_url.split("/")[-1]
            return "https://www.nature.com/articles/{}".format(nature_id)
        return None


def get_nature_info(content, journal_name):
    info_dict = {}
    if content.find("No abstract available") is not -1:
        return None
    if content.find("Sorry, the page you requested is unavailable. The link you requested might be broken, or no longer exist.") is not -1:
        return None
    soup = BeautifulSoup(content, features="html.parser")

    if journal_name == "Nature":
        breadcrumb = soup.find("li", {"id": "breadcrumb1"})
    else:
        breadcrumb = soup.find("li", {"id": "breadcrumb2"})
    info_dict["Type"] = breadcrumb.span.text

    title = soup.find("h1", {"class": "c-article-title"})
    info_dict["Title"] = title.text

    abstract = soup.find("div", {"id": "Abs1-content"})
    if abstract is None:
        return None
    info_dict["Abstract"] = abstract.text
    return info_dict


def __main__():
    scraper = Scraper()
    data_dir = "E:\\temp\\secret\\"

    journal_names = ["Nature", "Nature Methods", "Nature Communications", "Nature Biotechnology"]
    for journal_name in journal_names:
        # all docs
        for year in range(2010, 2021):
            print("Year:{} Journal: {}".format(year, journal_name))
            scraper.scrape_all(
                "{}{}\\{}".format(data_dir, journal_name, year),
                range(1, 1000),
                lambda x: "https://pubmed.ncbi.nlm.nih.gov/?term=((%22{}%2F01%2F01%22%5BDate%20-%20Publication" \
                "%5D%20%3A%20%22{}%2F12%2F31%22%5BDate%20-%20Publication%5D))%20AND%20(%22" \
                "{}%22%5BJournal%5D)&sort=&page={}".format(year, year, journal_name.replace(" ", "+"), x),
                early_stop_func=get_max_page
            )
            print("Index scraped.")

            base_path = "{}{}\\{}".format(data_dir, journal_name, year)
            if not path.isdir(base_path):
                continue
            doc_ids_path = base_path + ".pubmed.json"
            if path.isfile(doc_ids_path):
                with open(doc_ids_path, "r", encoding="utf-8") as doc_ids_file:
                    yearly_doc_ids = json.load(doc_ids_file)
                    doc_ids_file.close()
            else:
                yearly_doc_ids = []
                for base, dirs, files in os.walk(base_path):
                    for file in files:
                        with open(path.join(base, file), "r", encoding="utf-8") as index_file:
                            soup = BeautifulSoup(index_file.read(), features="html.parser")
                            index_file.close()
                            for item in soup.find_all("a", {"class": "docsum-title"}):
                                yearly_doc_ids.append(item["href"].strip("/"))
                with open(doc_ids_path, "w", encoding="utf-8") as doc_ids_file:
                    json.dump(yearly_doc_ids, doc_ids_file)
                    doc_ids_file.close()
            print("Detail page list parsed.")

            pubmed_dir = "{}{}\\all_pubmed".format(data_dir, journal_name)
            scraper.scrape_all(
                pubmed_dir,
                yearly_doc_ids,
                lambda x: "https://pubmed.ncbi.nlm.nih.gov/{}/".format(x)
            )
            print("Detail pages scraped.")

            source_dir = "{}{}\\all_source".format(data_dir, journal_name)
            scraper.scrape_all(
                source_dir,
                yearly_doc_ids,
                lambda x: get_source_link(x, pubmed_dir, journal_name)
            )
            print("Source pages scraped.\n")

            result_dir = "E:\\temp\\secret\\{}\\all_results".format(journal_name)

            if journal_name == "Nature":
                scraper.process_all(
                    input_func=lambda x: "{}\\{}.html".format(source_dir, x),
                    output_dir=result_dir,
                    target_names=yearly_doc_ids,
                    process_func=lambda x: get_nature_info(x, journal_name)
                )


if __name__ == "__main__":
    __main__()
