import json
import time
import traceback
from abc import ABC

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from unidecode import unidecode

from crawl.Crawler import Crawler as AbstractCrawler
from crawl.Crawler import WebDriver

mapping = {
    'watch': 'bc-filter-Watches',
    'bag': 'bc-filter-Bags'
}


class Crawler(AbstractCrawler, ABC):
    def __init__(self, category='Chanel', object_type='bag'):
        super().__init__(category, object_type)

        self.category = category

        if category == 'Dior':
            category = 'Christian Dior'

        self.crawlUrl = ('https://shop.rebag.com/collections/all-bags?pf_v_designers='
                         + category.replace(' ', '+')
                         + '&page=')

    def __crawl(self):
        page = 1
        items = []

        driver = WebDriver().chrome()

        while True:
            url = self.crawlUrl + str(page)
            print(url)

            driver.get(url)
            time.sleep(3)

            try:
                # wait page to load
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'plp__remove-filter-btn')))
                # scroll to bottom trigger image load
                driver.execute_script(WebDriver.SCROLL_TO_BOTTOM)
                # wait bottom pagination to display = scrolled bottom
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'plp__products-grid-pagination')))
                time.sleep(3)
            except TimeoutException:
                time.sleep(3)

            time.sleep(5)
            containers = driver.find_elements(By.CLASS_NAME, 'plp__product')
            print(len(containers))

            if len(containers) == 0:
                break

            for container in containers:
                soup = BeautifulSoup(container.get_attribute('innerHTML'), 'html.parser')
                try:
                    title = soup.find('span', {'class': 'products-carousel__card-title'}).get_text().strip()
                    title = unidecode(title)
                    title = self.sanitize_title(title)

                    if not self.is_valid_title(title):
                        continue

                    price_string = soup.find('span', {'class': 'rewards-plus-plp__product-price-value'}).get_text().strip()
                    price = float(''.join([s for s in price_string if s.isdigit()])) * 7.8

                    condition = soup.find('span', {'class': 'products-carousel__card-condition'}).get_text().strip()

                    image = soup.find('img').get('src')
                    product_id = soup.find('button', {'class': 'products-carousel__favorite-container'}).get(
                        'data-product-id')
                    try:
                        like = soup.find('span', {'class': 'products-carousel__favorite-container--counter'}).get_text().strip()
                        like = int(like)
                    except AttributeError:
                        like = 0

                    link_element = soup.find("a", class_="plp__card")
                    link = link_element["href"]

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
                        'url': link,
                        'id': self.source().lower() + '-' + product_id,
                        'productId': product_id,
                        'currency': self.currency(),
                        'image': image,
                        'folder': self.get_folder(title, model),
                        'condition': condition,
                        # 'keywords': AbstractCrawler.generate_keywords(title)
                        'size': size
                    })
                except AttributeError:
                    print(soup)
                    print(traceback.format_exc())

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
        return 'https://www.rebag.com'

    def source(self):
        return 'Rebag'

    def currency(self):
        return 'HKD'

    def file(self):
        return self.source() + '_' + self.object_type + '_' + self.category + '.json'
