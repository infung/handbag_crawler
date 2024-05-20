import json
import traceback
from abc import ABC

import requests
from bs4 import BeautifulSoup

from requests.adapters import HTTPAdapter, Retry
from unidecode import unidecode

from crawl.Crawler import Crawler as AbstractCrawler


class Crawler(AbstractCrawler, ABC):
    def __init__(self, category='Balenciaga', object_type='bag'):
        super().__init__(category, object_type)
        self.category = category
        if category == 'Valentino Garavani':
            category = 'Valentino'
        if category == 'Bvlgari':
            category = 'Bulgari'
        self.crawlUrl = 'https://www.collectorsquare.com/en/bags/' + category.lower().replace(' ', '-') + '/?page='
        self.object_type = object_type

    def __crawl(self):
        page = 1
        count = 0

        items = []

        while True:
            url = self.crawlUrl + str(page)
            print(url)

            r = requests.get(url=url)
            soup = BeautifulSoup(r.content, features='html.parser')
            try:
                containers = soup.findAll('li', {'class': 'product'})
                if len(containers) == 0:
                    break

                print(len(containers))

                for container in containers:
                    model = container.find('p', {'class': 'collection'}).get_text().replace('\n', '').strip()
                    title = container.find('div', {'class': 'name'}).get_text().replace('\n', '').strip()
                    title = unidecode(title)
                    if self.category == 'Bvlgari':
                        title = title.replace("Bulgari", "Bvlgari").replace("bulgari", "bvlgari")
                    image = container.find('img').get('data-src')
                    price_string = container.find('p', {'class': 'price-cs'}).get_text()
                    currency = soup.find('span', itemprop='priceCurrency').text
                    href = self.url() + container.find('div', {'class': 'image-holder'}).find('a').get('href')
                    try:
                        price = float(''.join([s for s in price_string if s.isdigit()]))
                    except ValueError:
                        # member only product
                        price = 0

                        json_obj = json.loads(container.find('div', {'class': 'image-holder'}).find('a').get('data-ajax-popin-request-data'))
                        href = self.url() + json_obj['targetUrl'].replace('\\', '')
                        continue

                    if model == '':
                        continue

                    # if currency == '£':
                    price *= 9.91
                    print(price)

                    product_id = container.get('data-product-code')
                    item = {
                        'brand': self.category,
                        'href': href,
                        'price': price,
                        'trends': [{
                            'date': self.get_date(),
                            'price': price
                        }],
                        'title': title,
                        'like': 0,
                        'source': self.source(),
                        'url': href,
                        'id': self.source().lower() + '-' + product_id,
                        'productId': product_id,
                        'currency': self.currency(),
                        'image': image,
                        'condition': 'Pre-Owned',
                        # 'keywords': AbstractCrawler.generate_keywords(title)
                    }

                    # product detail page
                    inner_r = requests.get(url=href)
                    inner_soup = BeautifulSoup(inner_r.content, features='html.parser')
                    detail_div = inner_soup.find('div', {'class': 'secondary-info'})
                    columns = detail_div.findAll('div', {'class': 'col-xs-6'})
                    column1_rows = columns[0].findAll('div')
                    column2_rows = columns[1].findAll('div')

                    for row in column1_rows:
                        whole_text = row.get_text().replace('\n', '').strip()
                        try:
                            span_text = row.find('span').get_text().replace('\n', '').strip()
                        except AttributeError:
                            continue
                        field_key = whole_text.replace(span_text, '').replace(' : ', '').lower().strip()
                        field_value = span_text
                        if 'condition                                        condition rate' == field_key:
                            item['condition'] = field_value
                        if field_key in ['collection', 'color', 'material', 'category']:
                            item[field_key] = field_value

                    for row in column2_rows:
                        whole_text = row.get_text().replace('\n', '').strip()
                        span_text = row.find('span').get_text().replace('\n', '').strip()
                        field_key = whole_text.replace(span_text, '').replace(' : ', '').lower().strip()
                        field_value = span_text
                        if 'condition                                        condition rate' == field_key:
                            item['condition'] = field_value
                        if field_key in ['collection', 'color', 'material', 'category']:
                            item[field_key] = field_value

                    if 'collection' not in item:
                        continue

                    item['collection'] = unidecode(item['collection'])
                    item['collection'] = self.get_bag_collection(item['collection'])

                    if item['collection'] is None:
                        continue

                    item['folder'] = self.get_folder(title, item['collection'])
                    item['size'] = self.get_bag_size(title, item['collection'])

                    if 'color' in item:
                        item['color'] = self.get_bag_detail(item['color'], 'color')
                    else:
                        item['color'] = self.get_bag_detail(title,'color')

                    if 'category' in item:
                        item['category'] = self.get_bag_detail(item['category'], 'category')
                    else:
                        item['category'] = self.get_bag_detail(title,'category')

                    if 'material' in item:
                        item['material'] = self.get_bag_detail(item['material'], 'material')
                    else:
                        item['material'] = self.get_bag_detail(title,'material')

                    if item['color'] is not None and item['category'] is not None and item['material'] is not None and item['size'] is not None:
                        print(item['collection'] + '-' + item['color'] + '-' + item['category'] + '-' + item['material'] + '-' + item['condition'] + '-' + item['size'])

                    items.append(item)
            except AttributeError:
                print(title)
                print(href)
                print(traceback.format_exc())

            page += 1

        return items

    def start(self):
        self.all_items = self.__crawl()
        print('Crawled ' + str(len(self.all_items)) + ' items')

        with open(self.file(), 'w', encoding='utf-8') as output_file:
            json.dump(self.all_items, output_file, ensure_ascii=False, indent=4)


    def url(self):
        return 'https://www.collectorsquare.com'

    def source(self):
        return 'CollectorSquare'

    def currency(self):
        return 'HKD'

    def file(self):
        return self.source() + '_' + self.object_type + '_' + self.category + '.json'
