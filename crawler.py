from bs4 import BeautifulSoup
from multiprocessor import MultiProcessor
from os import path
import json
import os

class NatureCrawler:
    def __init__(self, processor: MultiProcessor, journal_name: str, data_dir: str):
        self._processor = processor
        self._journal_name = journal_name
        self._data_dir = data_dir

    @property
    def journal_name(self) -> str:
        return self._journal_name

    @property
    def data_dir(self) -> str:
        return self._data_dir

    @property
    def processor(self) -> MultiProcessor:
        return self._processor

    @classmethod
    def get_max_page(cls, content):
        soup = BeautifulSoup(content, features="html.parser")
        amount = soup.find("div", {"class": "results-amount"})
        item_count = int(amount.span.text.replace(',', ''))
        return divmod(item_count, 10)[0] + 1

    @classmethod
    def get_source_link(cls, i, base_dir):
        with open("{}\\{}.html".format(base_dir, i), "r", encoding="utf-8") as pubmed_file:
            content = pubmed_file.read()
            if content.find("full-text-links-list") is -1:
                return None
            pubmed_file.close()
            soup = BeautifulSoup(content, features="html.parser")
            link_div = soup.find("div", {"class": "full-text-links-list"})
            if link_div is None:
                return None
        return cls.parse_origin_link(link_div.a["href"])

    @classmethod
    def parse_origin_link(cls, origin_url):
        nature_id = origin_url.split("nature")[-1]
        return "https://www.nature.com/articles/nature{}".format(nature_id)

    def get_info(self, content):
        info_dict = {}
        if content.find("No abstract available") is not -1:
            return None
        if content.find("Sorry, the page you requested is unavailable. "
                        "The link you requested might be broken, or no longer exist.") is not -1:
            return None
        soup = BeautifulSoup(content, features="html.parser")

        info_dict["Type"] = self.get_doc_type(soup)

        title = soup.find("h1", {"class": "c-article-title"})
        info_dict["Title"] = title.text

        abstract = soup.find("div", {"id": "Abs1-content"})
        if abstract is None:
            return None
        info_dict["Abstract"] = abstract.text
        return info_dict

    @classmethod
    def get_doc_type(cls, soup):
        breadcrumb = soup.find("li", {"id": "breadcrumb1"})
        return breadcrumb.span.text

    def scrape_index(self, year):
        self.processor.scrape_all(
            "{}{}\\{}".format(self.data_dir, self.journal_name, year),
            range(1, 1000),
            lambda x: "https://pubmed.ncbi.nlm.nih.gov/?term=((%22{}%2F01%2F01%22%5BDate%20-%20Publication" \
                      "%5D%20%3A%20%22{}%2F12%2F31%22%5BDate%20-%20Publication%5D))%20AND%20(%22" \
                      "{}%22%5BJournal%5D)&sort=&page={}".format(year, year, self.journal_name.replace(" ", "+"), x),
            early_stop_func=self.get_max_page
        )
        print("Index scraped.")
        return True

    def extract_detail_page_list(self, year):
        base_path = "{}{}\\{}".format(self.data_dir, self.journal_name, year)
        if not path.isdir(base_path):
            return None
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
        return yearly_doc_ids

    @property
    def pubmed_dir(self) -> str:
        return "{}{}\\all_pubmed".format(self.data_dir, self.journal_name)

    @property
    def source_dir(self) -> str:
        return "{}{}\\all_source".format(self.data_dir, self.journal_name)

    def scrape_pubmed_detail_pages(self, yearly_doc_ids):
        self.processor.scrape_all(
            self.pubmed_dir,
            yearly_doc_ids,
            lambda x: "https://pubmed.ncbi.nlm.nih.gov/{}/".format(x)
        )
        print("Detail pages scraped.")

    def scrape_source_detail_pages(self, yearly_doc_ids):
        self.processor.scrape_all(
            self.source_dir,
            yearly_doc_ids,
            lambda x: self.get_source_link(x, self.pubmed_dir)
        )
        print("Source pages scraped.\n")

    def extract_info(self, yearly_doc_ids):
        result_dir = "E:\\temp\\secret\\{}\\all_results".format(self.journal_name)

        self.processor.process_all(
            input_func=lambda x: "{}\\{}.html".format(self.source_dir, x),
            output_dir=result_dir,
            target_names=yearly_doc_ids,
            process_func=lambda x: self.get_info(x)
        )


class NatureSubCrawler(NatureCrawler):
    def __init__(self, processor: MultiProcessor, journal_name: str, data_dir: str):
        super(NatureSubCrawler, self).__init__(processor, journal_name, data_dir)

    @classmethod
    def parse_origin_link(cls, origin_url):
        nature_id = origin_url.split("/")[-1]
        return "https://www.nature.com/articles/{}".format(nature_id)

    @classmethod
    def get_doc_type(cls, soup):
        breadcrumb = soup.find("li", {"id": "breadcrumb2"})
        return breadcrumb.span.text


def __main__():
    processor = MultiProcessor()
    data_dir = "E:\\temp\\secret\\"

    journal_names = ["Nature", "Nature Methods", "Nature Communications", "Nature Biotechnology"]
    for journal_name in journal_names:
        if journal_name == "Nature":
            crawler = NatureCrawler(processor, journal_name, data_dir)
        elif journal_name.startswith("Nature"):
            crawler = NatureSubCrawler(processor, journal_name, data_dir)
        else:
            continue

        # all docs
        for year in range(2010, 2021):
            print("Year:{} Journal: {}".format(year, journal_name))
            crawler.scrape_index(year)

            yearly_doc_ids = crawler.extract_detail_page_list(year)
            if yearly_doc_ids is None:
                continue

            crawler.scrape_pubmed_detail_pages(yearly_doc_ids)
            crawler.scrape_source_detail_pages(yearly_doc_ids)

            crawler.extract_info(yearly_doc_ids)


if __name__ == "__main__":
    __main__()
