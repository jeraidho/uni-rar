import requests
from fake_useragent import UserAgent
from tqdm import tqdm
import time
from bs4 import BeautifulSoup
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd


class Crawler:
    def __init__(self, url=None):
        self.session = requests.session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        self.user_agent = UserAgent()

        # main url
        if not url:
            self.url = 'https://typo.uni-konstanz.de/rara/category/raritaetenkabinett/page/'
        else:
            self.url = url

        # filename and time for backup while crawling
        self.moment = time.strftime("%Y-%b-%d_%H_%M_%S", time.localtime())
        self.errors = []
        self.database = {}
        self.counter = 0

    def get_links(self, page_url):
        try:
            req = self.session.get(page_url, headers={'User-Agent': self.user_agent.random}, timeout=10)
        except Exception as e:
            self.errors.append(e)
        else:
            soup = BeautifulSoup(req.text, 'html.parser')
            links = [item.attrs['href'] for item in soup.find_all('a', class_="more-link")]
            return links

    def render_page(self, page_url):
        try:
            req = self.session.get(page_url, headers={'User-Agent': self.user_agent.random}, timeout=10)
        except Exception as e:
            self.errors.append(e)
        else:
            soup = BeautifulSoup(req.text, 'html.parser')
            title = soup.find('h1', class_="post-title").text.split()
            if title[0] == "Universal":
                entity, number = title[0], title[1].strip()[:-1]
            else:
                entity, number = ' '.join(title[:-1]), title[-1]
            text = "".join(soup.find('div', class_="post-content").find('h3').text.split(': ')[1:])
            rendered = {
                'url': page_url,
                'entity': entity.lower(),
                'id': int(number),
                'text': text
            }
            all_attributes = soup.find_all('dl')
            for attributes in all_attributes:
                attribute_lines = str(attributes).split('\n')
                index = 0
                while index < len(attribute_lines):
                    attribute_soup = BeautifulSoup(attribute_lines[index], 'html.parser')
                    if attribute_soup.find('dt'):
                        attribute_title = '_'.join(attribute_soup.find('dt').get_text().lower().split())
                        if attribute_title == 'universals_violated':
                            attribute_value = ", ".join([link.attrs['href'] for link in
                                                         BeautifulSoup(attribute_lines[index + 1],
                                                                       'html.parser').find_all('a')])
                        else:
                            attribute_value = BeautifulSoup(attribute_lines[index + 1],
                                                            'html.parser').find('dd').text
                        rendered[attribute_title] = attribute_value.strip()
                    index += 1

            return rendered

    def crawl(self, pages_number):
        for page_number in tqdm(range(1, pages_number + 1), desc="Page processing", colour="green"):
            page_links = self.get_links(self.url + str(page_number) + '/')
            for page_link in tqdm(page_links, desc="Item processing", colour="red"):
                processed = self.render_page(page_link)
                self.database[self.counter] = processed
                self.counter += 1

        print('Done!')
        print(self.errors if self.errors else "Everything fine")
        return self

    def _check_filename(filetype: str):
        def decorator(func):
            def wrapper(*args, **kwargs):
                filename = kwargs['filename']
                if not filename:
                    moment = time.strftime("%Y-%b-%d_%H_%M_%S", time.localtime())
                    filename = "crawled-" + moment + ".json"
                elif not filename.endswith(filetype):
                    filename += filetype
                kwargs["filename"] = filename
                return func(*args, **kwargs)

            return wrapper

        return decorator

    @_check_filename('.json')
    def save_json(self, filename=None):
        with open(filename, 'w') as f:
            json.dump(self.database, f)
        return self

    @_check_filename('.csv')
    def save_csv(self, filename=None, sep=','):
        pd.DataFrame.from_dict(self.database, orient="index").to_csv(filename, sep=sep, index=False)

        return self

    def DataFrame(self):
        return pd.DataFrame.from_dict(self.database, orient="index")

    @_check_filename('.json')
    def from_json(self, filename=None):
        with open(filename, 'r') as f:
            self.database = json.load(f)
        self.counter = max(map(int, self.database.keys()))
        self.errors = []

        return self

    def __add__(self, other):
        if not isinstance(other, Crawler):
            raise TypeError('Only Crawler instance can be added')
        for index, item in enumerate(other.database.values()):
            self.database[self.counter + index + 1] = item
        self.counter += (index + 1)

        return self

    def __len__(self):
        return self.counter + 1


if __name__ == "__main__":
    # example
    crawler = Crawler(url="https://typo.uni-konstanz.de/rara/author/fp/page/").crawl(pages_number=218).save_json(filename="fp_posted.json")
    crawler.save_csv(filename="fp_posted")
    # crawler = Crawler().from_json(filename="two_pages")
    # crawler.save_csv(filename='two_pages_no_index')

    # double = pd.read_csv('rarities_double.csv')
    # rarities = pd.read_csv('rarities.csv')
    # print(len(set(double.url) & set(rarities.url)))
