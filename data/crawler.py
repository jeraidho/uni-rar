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

        # filename and time for backup while crawling
        self.moment = time.strftime("%Y-%b-%d_%H_%M_%S", time.localtime())
        self.errors = []
        self.database = {}

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
            entity, number = ''.join(title[:-1]), title[-1]
            text = "".join(soup.find('div', class_="post-content").find('h3').text.split(': ')[1:])
            rendered = {
                'url': page_url,
                'entity': entity,
                'id': number,
                'text': text
            }
            attributes = soup.find('dl')

            attribute_lines = str(attributes).split('\n')
            index = 0
            while index < len(attribute_lines):
                attribute_soup = BeautifulSoup(attribute_lines[index], 'html.parser')
                if attribute_soup.find('dt'):
                    attribute_title = '_'.join(attribute_soup.find('dt').get_text().lower().split())
                    if attribute_title == 'universals_violated':
                        attribute_value = ", ".join([link.attrs['href'] for link in
                                                     BeautifulSoup(attribute_lines[index + 1], 'html.parser').find_all(
                                                         'a')])
                    else:
                        attribute_value = BeautifulSoup(attribute_lines[index + 1], 'html.parser').find('dd').text
                    rendered[attribute_title] = attribute_value.strip()
                index += 1

            return rendered

    def crawl(self, pages_number, entity_='rarities'):
        counter = 0
        for page_number in tqdm(range(1, pages_number + 1), desc="Page processing", colour="green"):
            page_links = self.get_links(self.url + str(page_number) + '/')
            for page_link in tqdm(page_links, desc="Item processing", colour="red"):
                processed = self.render_page(page_link)
                self.database[counter] = processed
                counter += 1

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

        return self


if __name__ == "__main__":
    # example
    crawler = Crawler().crawl(pages_number=2).save_json(filename="two_pages")
    crawler.save_csv(filename="two_pages")
    crawler = Crawler().from_json(filename="two_pages")
    crawler.save_csv(filename='two_pages_no_index')
