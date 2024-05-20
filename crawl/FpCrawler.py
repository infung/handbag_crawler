import json
import time
import traceback
from abc import ABC

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from unidecode import unidecode

from crawl.Crawler import Crawler as AbstractCrawler
from crawl.Crawler import WebDriver


class Crawler(AbstractCrawler, ABC):
    def __init__(self, category='Rolex', object_type='watch'):
        super().__init__(category, object_type)

        self.category = category

        if category == 'Bvlgari':
            category = 'Bulgari'

        self.crawlUrl = 'https://www.fashionphile.com/shop/handbag-styles/handbags?brands=' \
                        + category.lower().replace(' ', '-') \
                        + '&pageSize=180&sort=date-desc&page='
        if object_type == 'watch':
            self.crawlUrl = 'https://www.fashionphile.com/shop/categories/watches?brands=' \
                            + category.lower().replace(' ', '-') \
                            + '&pageSize=180&sort=date-desc&page='


    def __crawl(self):
        page = 1
        items = []

        driver = WebDriver().chrome()

        while True:
            url = self.crawlUrl + str(page)
            print(url)

            driver.get(url)
            time.sleep(3)

            tags = driver.find_elements(By.XPATH, '//button[@class="appliedFilterButton"]')
            if len(tags) == 0:
                break

            time.sleep(5)
            containers = driver.find_elements(By.XPATH, '//div[@class="product"]')
            # favs = driver.find_elements(By.XPATH, '//span[contains(@class, "fp-fav-count")]')
            # while len(containers) > len(favs):
            #     time.sleep(3)
            #     favs = driver.find_elements(By.XPATH, '//span[@class="fp-fav-count"]')

            # print(len(containers))
            if len(containers) > 1:
                for container in containers:
                    soup = BeautifulSoup(container.get_attribute('innerHTML'), 'html.parser')
                    try:
                        title = self.category + ' ' + soup.find('p', {'class': 'productTitle'}).get_text().strip()
                        title = unidecode(title)
                        title = self.sanitize_title(title)

                        if self.category == 'Bvlgari':
                            title = title.replace("Bulgari", "Bvlgari").replace("bulgari", "bvlgari")

                        if not self.is_valid_title(title):
                            continue

                        price_string = soup.find('span', {'itemprop': 'price'}).get_text().strip()
                        price = float(''.join([s for s in price_string if s.isdigit()])) * 7.8

                        condition = soup.find('p', {'class': 'condition'}).get_text().strip().replace("Condition: ", "")

                        image = soup.find('img').get('src')
                        link = soup.find('a').get('href')
                        parts = soup.find('a').get('href').split('-')
                        product_id = parts[len(parts) - 1]

                        try:
                            like = soup.find('span', {'class': 'fp-favCount'}).get_text().strip()
                            like = int(like)
                        except AttributeError:
                            like = 0

                        model = self.get_bag_collection(title)
                        color = self.get_bag_detail(title, 'color')
                        cat = self.get_bag_detail(title, 'category')
                        material = self.get_bag_detail(title, 'material')
                        if model is None:
                            continue

                        size = self.get_bag_size(title, model)

                        if color is not None and cat is not None and material is not None and size is not None:
                            print(model + '-' + color + '-' + cat + '-' + material + '-' + condition + '-' + size)

                        items.append({
                            'brand': self.category,
                            'collection': model,
                            'price': price,
                            'color': color,
                            'category': cat,
                            'material': material,
                            'trends': [{
                                'date': self.get_date(),
                                'price': price
                            }],
                            'title': title,
                            'like': like,
                            'source': self.source(),
                            'url': self.url() + link,
                            'id': self.source().lower() + '-' + product_id,
                            'productId': product_id,
                            'currency': self.currency(),
                            'image': image,
                            'folder': self.get_folder(title, model),
                            'condition': condition,
                            'size': size
                            # 'keywords': AbstractCrawler.generate_keywords(title)
                        })
                    except AttributeError:
                        print(title)
                        print(traceback.format_exc())
            else:
                break

            page += 1

        driver.close()
        driver.quit()

        return items

    def start(self):
        self.all_items = self.__crawl()
        print('Crawled ' + str(len(self.all_items)) + ' items')

        with open(self.file(), 'w', encoding='utf-8') as output_file:
            json.dump(self.all_items, output_file, ensure_ascii=False, indent=4)

    def url(self):
        return 'https://www.fashionphile.com'

    def source(self):
        return 'Fashionphile'

    def currency(self):
        return 'HKD'

    def file(self):
        return self.source() + '_' + self.object_type + '_' + self.category + '.json'
