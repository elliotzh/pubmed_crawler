import urllib3
import datetime
from os import path
import os
import threading
import json
from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
import requests
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MultiProcessor:
    def __init__(self, processes=20, chunk_size=100):
        self._thread_pool = ThreadPool(processes=processes)
        self._chunk_size = chunk_size

        # temp
        self._local_dir = ""
        self._url_func = lambda x: x
        self._target_suffix = ""

        self._process_func = lambda x: x
        self._reprocess = False
        self._need_redirect = False

    def scrape_all(self, local_dir, target_names, url_func, early_stop_func=None, target_suffix="html", need_redirect=False):
        self._url_func = url_func
        self._local_dir = local_dir
        self._target_suffix = target_suffix
        self._need_redirect = need_redirect

        if not path.isdir(self._local_dir):
            os.makedirs(self._local_dir)

        max_i = -1
        if early_stop_func is not None:
            self.scrape_one(target_names[0])
            if not path.isfile(self.get_target_path(target_names[0])):
                return
            with open(self.get_target_path(target_names[0]), "r", encoding="utf-8") as first_page_file:
                max_i = early_stop_func(first_page_file.read())
                first_page_file.close()
            assert max_i <= 1000
            if max_i == 0:
                return

        for i, _ in enumerate(self._thread_pool.imap(self.scrape_one, target_names[:max_i], chunksize=self._chunk_size)):
            if divmod(i, 1000)[1] == 0:
                print("#{}: {}".format(i, datetime.datetime.now()))

    def get_target_path(self, target_name):
        return path.join(self._local_dir, "{}.{}".format(target_name, self._target_suffix))

    def scrape_one(self, target_name):
        if path.isfile(self.get_target_path(target_name)):
            return target_name

        s = requests.Session()
        target_url = self._url_func(target_name)
        if target_url is None:
            print("Can't find source link for pubmed doc id {}".format(target_name))
            return target_name

        try:
            if self._need_redirect:
                target_url = self.expand_url(target_url)
            r = s.get(target_url, allow_redirects=1, verify=False)
            assert r.status_code == 200
        except:
            exc_type, e, trace = sys.exc_info()
            print("met {}({}) in request for pubmed doc {}".format(str(exc_type), str(e), target_name))
            return target_name
        with open(self.get_target_path(target_name), "wb") as html_file:
            html_file.write(r.content)
            html_file.close()
        return target_name

    def process_one(self, target_name):
        output_path = path.join(self._local_dir, "{}.{}".format(target_name, self._target_suffix))
        if self._reprocess is False and path.isfile(output_path):
            return
        obj = self._process_func(target_name)
        with open(output_path, "w", encoding="utf-8") as outfile:
            json.dump(obj, outfile, indent=2)
            outfile.close()

    def process_all(self, target_names, process_func, output_dir, target_suffix="json"):
        self._process_func = process_func
        self._local_dir = output_dir
        self._target_suffix = target_suffix

        if not path.isdir(self._local_dir):
            os.makedirs(self._local_dir)

        if len(target_names) is not 0:
            existed_first_output_path = path.join(self._local_dir, "{}.{}".format(target_names[0], self._target_suffix))
            if path.isfile(existed_first_output_path):
                with open(existed_first_output_path, "r", encoding="utf-8") as cache_file:
                    old = cache_file.read()
                    cache_file.close()
                self._reprocess = True
                self.process_one(target_name=target_names[0])
                with open(existed_first_output_path, "r", encoding="utf-8") as cache_file:
                    new = cache_file.read()
                    cache_file.close()
                self._reprocess = (old != new)

        for i, _ in enumerate(
                self._thread_pool.imap(self.process_one, target_names, chunksize=self._chunk_size)):
            if divmod(i, 1000)[1] == 0:
                print("#{}: {}".format(i, datetime.datetime.now()))

    @classmethod
    def expand_url(cls, url):
        s = requests.Session()
        try:
            r = s.get(url.rstrip(), allow_redirects=1, verify=False)
            return r.url.rstrip()
        except requests.exceptions.ConnectionError as e:
            print(e)

