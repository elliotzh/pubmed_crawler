from bs4 import BeautifulSoup
from multiprocessor import MultiProcessor
from os import path
import json
import os
import csv
from typing import Dict, List, Optional


class ArticleInfo:
    def __init__(self, pubmed_id, journal_name):
        self.pubmed_id = pubmed_id
        self.journal_name = journal_name
        self.abstract = ""
        self.article_type = "Unknown"
        self.title = "Unknown"
        self.meta = "Unknown"
        self.date = "Unknown"
        self.subject = "Unknown"
        self.publication_type = "Unknown"

    @property
    def is_valid(self):
        for content in [
            # self.abstract,
            # self.article_type,
            self.title,
            # self.meta
        ]:
            if content != "Unknown":
                return True
        return False

    def dump(self):
        return {
            "Journal": self.journal_name,
            "Id": self.pubmed_id,
            "PublicationType": self.publication_type,
            "Type": self.article_type,
            "Subject": self.subject,
            "Title": self.title,
            "Abstract": self.abstract,
            "Date": self.date,
            "Meta": self.meta
        }

    @classmethod
    def fieldnames(cls):
        return ["Journal", "Id", "PublicationType", "Type", "Subject", "Title", "Abstract", "Date", "Meta"]


class PubMedCrawler:
    def __init__(self, processor: Optional[MultiProcessor], journal_name: str, data_dir: str):
        self._processor = processor
        if processor is None:
            self.reset_processor()

        self._journal_name = journal_name
        self._data_dir = data_dir

    def reset_processor(self):
        self._processor = MultiProcessor()

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
        try:
            item_count = int(amount.span.text.replace(',', ''))
        except AttributeError:
            return 0
        return divmod(item_count, 10)[0] + 1

    @classmethod
    def get_source_link(cls, i, base_dir):
        pubmed_path = "{}\\{}.html".format(base_dir, i)
        if path.isfile(pubmed_path):
            with open(pubmed_path, "r", encoding="utf-8") as pubmed_file:
                content = pubmed_file.read()
                if content.find("full-text-links-list") is -1:
                    return None
                pubmed_file.close()
                soup = BeautifulSoup(content, features="html.parser")
                link_div = soup.find("div", {"class": "full-text-links-list"})
                if link_div is None:
                    return None
            return link_div.a["href"]
        else:
            return None

    @classmethod
    def parse_origin_link(cls, origin_url):
        raise NotImplementedError()

    def extract_info(self, target_name) -> ArticleInfo:
        article_info = ArticleInfo(target_name, self.journal_name)
        pubmed_path = "{}\\{}.html".format(self.pubmed_dir, target_name)
        if not path.isfile(pubmed_path):
            return article_info

        with open(pubmed_path, "r", encoding="utf-8") as pubmed_file:
            soup = BeautifulSoup(pubmed_file.read(), features="html.parser")
            pubmed_file.close()
        major_content = soup.find("main", {"class": "article-details"})
        if major_content is None:
            return article_info
        article_info.title = major_content.find("h1", {"class": "heading-title"}).text.strip()
        origin_meta = major_content.find("span", {"class": "cit"}).text.strip()
        try:
            article_info.date, article_info.meta = origin_meta.split(";", 1)
        except ValueError:
            article_info.meta = origin_meta

        abstract_tag = major_content.find("div", {"id": "enc-abstract"})
        if abstract_tag is not None:
            article_info.abstract = abstract_tag.text.strip()

        publication_type_tag = major_content.find("div", {"class": "publication-type"})
        if publication_type_tag is not None:
            article_info.publication_type = publication_type_tag.text.strip()

        source_path = "{}\\{}.html".format(self.source_dir, target_name)
        if path.isfile(source_path):
            try:
                with open(source_path, "r", encoding="utf-8") as source_file:
                    soup = BeautifulSoup(source_file.read(), features="html.parser")
                    self.update_info_from_source(article_info, soup)
            except UnicodeDecodeError:
                article_info.article_type = "DecodeError"

        return article_info

    def update_info_from_source(self, article_info: ArticleInfo, content):
        pass

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

    def extract_detail_page_list(self, year) -> Optional[List[str]]:
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

    @property
    def result_dir(self) -> str:
        return "{}{}\\all_results".format(self.data_dir, self.journal_name)

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
            lambda x: self.get_source_link(x, self.pubmed_dir),
            need_redirect=True
        )
        print("Source pages scraped.\n")

    def extract_info_for_all(self, yearly_doc_ids, for_test=False):
        if for_test is True:
            all_articles = []
            for doc_id in yearly_doc_ids:
                all_articles.append(self.extract_info(doc_id))

            with open(path.join(self.data_dir, self.journal_name, "results.csv"), "w", encoding="utf-8", newline="") as results_file:
                writer = csv.DictWriter(results_file, ArticleInfo.fieldnames())
                writer.writeheader()
                for article in all_articles:
                    writer.writerow(article.dump())
                results_file.close()
        else:
            self.processor.process_all(
                process_func=lambda x: self.extract_info(x).dump(),
                target_names=yearly_doc_ids,
                output_dir=path.join(self.data_dir, self.journal_name, "all_results")
            )

    def merge_results(self):
        with open(path.join(self.data_dir, "{}.csv".format(self.journal_name)), "w", encoding="utf-8",
                  newline="") as results_file:
            writer = csv.DictWriter(results_file, ArticleInfo.fieldnames())
            writer.writeheader()
            for base, dirs, files in os.walk(self.result_dir):
                for file in files:
                    with open(path.join(base, file), "r", encoding="utf-8") as json_file:
                        try:
                            obj = json.load(json_file)
                        except json.decoder.JSONDecodeError:
                            target_name = file.split(".")[0]
                            obj = self.extract_info(target_name).dump()
                        writer.writerow(obj)
                        json_file.close()
            results_file.close()


class NatureCrawler(PubMedCrawler):
    def __init__(self, processor: Optional[MultiProcessor], journal_name: str, data_dir: str):
        super(NatureCrawler, self).__init__(processor, journal_name, data_dir)

    @classmethod
    def parse_origin_link(cls, origin_url):
        nature_id = origin_url.split("nature")[-1]
        return "https://www.nature.com/articles/nature{}".format(nature_id)

    def update_info_from_source(self, article_info: ArticleInfo, soup):
        breadcrumb = soup.find("li", {"id": "breadcrumb1"})
        if breadcrumb is not None:
            article_info.article_type = breadcrumb.span.text


class NatureSubCrawler(NatureCrawler):
    def __init__(self, processor: Optional[MultiProcessor], journal_name: str, data_dir: str):
        super(NatureSubCrawler, self).__init__(processor, journal_name, data_dir)

    @classmethod
    def parse_origin_link(cls, origin_url):
        nature_id = origin_url.split("/")[-1]
        return "https://www.nature.com/articles/{}".format(nature_id)

    def update_info_from_source(self, article_info: ArticleInfo, soup):
        breadcrumb = soup.find("li", {"id": "breadcrumb2"})
        if breadcrumb is not None:
            article_info.article_type = breadcrumb.span.text


class ScienceCrawler(PubMedCrawler):
    def __init__(self, processor: Optional[MultiProcessor], journal_name: str, data_dir: str):
        super(ScienceCrawler, self).__init__(processor, journal_name, data_dir)

    @classmethod
    def parse_origin_link(cls, origin_url):
        nature_id = origin_url.split("nature")[-1]
        return "https://www.nature.com/articles/nature{}".format(nature_id)

    def update_info_from_source(self, article_info: ArticleInfo, soup):
        header = soup.find("header", {"class": "article__header"})
        overline = header.find("div", {"class": "overline"})
        if overline is not None:
            article_type = overline.find("span", {"class": "overline__section"})
            if article_type is None:
                article_info.article_type = overline.text.strip()
            else:
                article_info.article_type = article_type.text
                article_info.subject = overline.find("span", {"class": "overline__subject"}).text.strip()


def __main__():
    data_dir = "E:\\temp\\secret\\"

    journal_names = [
        "Science advances",
        "Science",
        "Science signaling",
        "Science Translational Medicine",
        "Nature",
        "Nature Methods",
        "Nature Communications",
        "Nature Biotechnology",
    ]
    for journal_name in journal_names:
        if journal_name == "Nature":
            crawler = NatureCrawler(None, journal_name, data_dir)
        elif journal_name.startswith("Nature"):
            crawler = NatureSubCrawler(None, journal_name, data_dir)
        elif journal_name.startswith("Science"):
            crawler = ScienceCrawler(None, journal_name, data_dir)
        else:
            continue

        # all docs
        for year in range(2020, 2009, -1):
            crawler.reset_processor()
            print("Year:{} Journal: {}".format(year, journal_name))
            crawler.scrape_index(year)

            yearly_doc_ids = crawler.extract_detail_page_list(year)
            if yearly_doc_ids is None:
                continue

            crawler.scrape_pubmed_detail_pages(yearly_doc_ids)
            crawler.scrape_source_detail_pages(yearly_doc_ids)

            crawler.extract_info_for_all(yearly_doc_ids)

        crawler.merge_results()

    with open(path.join(data_dir, "merged.csv"), "w", encoding="utf-8", newline="") as merged_file:
        writer = csv.DictWriter(merged_file, ArticleInfo.fieldnames())
        writer.writeheader()
        for journal_name in journal_names:
            with open(path.join(data_dir, "{}.csv".format(journal_name)), "r", encoding="utf-8") as journal_file:
                reader = csv.DictReader(journal_file)
                for row in reader:
                    writer.writerow(row)
                journal_file.close()
        merged_file.close()


if __name__ == "__main__":
    __main__()
