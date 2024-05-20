import json
from abc import ABC

import requests
from unidecode import unidecode

from crawl.Crawler import Crawler as AbstractCrawler

mapping = {
    'watch': ['8', '12'],
    'bag': ['1891']
}


class Crawler(AbstractCrawler, ABC):
    def __init__(self, category='Rolex', object_type='watch'):
        super().__init__(category, object_type)
        self.category = category
        if category == 'Valentino Garavani':
            category = 'Valentino'
        category = category.replace(' ', '+')
        self.crawlUrl = 'https://api.truefacet.com/api/products/search?order=newarrival&manufacturer[0]=' \
                        + category.replace(' ', '+') \
                        + '&section=marketplace&format=json&p='
        self.mediaUrl = 'https://media.truefacet.com/media/catalog/product'
        self.object_type = object_type


    def __crawl(self, initial_index, cat_id):
        page = 1
        count = initial_index
        items = []

        while True:
            url = self.crawlUrl + str(page) + '&categoryId[0]=' + cat_id
            print(url)

            r = requests.get(url=url)
            data = r.json()
            data_list = data['data']['hits']['hits']
            if len(data_list) > 0:
                for item in data_list:
                    try:
                        title = unidecode(item['_source']['name_en'])
                        title = self.sanitize_title(title)

                        if not self.is_valid_title(title):
                            continue

                        price = float(item['_source']['price']) * 7.8
                        product_id = item['_id'].replace('|1', '')
                        model = self.get_bag_collection(title)
                        color = self.get_bag_detail(title, 'color')
                        cat = self.get_bag_detail(title, 'category')
                        material = self.get_bag_detail(title, 'material')
                        if model is None:
                            continue

                        try:
                            condition = item['_source']['options_condition_en']
                        except KeyError:
                            condition = 'Never Worn'

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
                            'like': 0,
                            'source': self.source(),
                            'url': self.url() + '/' + item['_source']['url_path_en'],
                            'id': self.source().lower() + '-' + product_id,
                            'productId': product_id,
                            'image': 'https://media.truefacet.com/media/catalog/product' + item['_source']['image'],
                            'folder': self.get_folder(title, model),
                            'currency': self.currency(),
                            # 'keywords': AbstractCrawler.generate_keywords(title),
                            'condition': condition,
                            'size': size
                        })
                        count += 1
                    except AttributeError as e:
                        print(item)
                        print(e)
            else:
                break

            page += 1

        return items

    def start(self):
        all_items = []

        for cat_id in mapping[self.object_type]:
            all_items += self.__crawl(len(all_items), cat_id)
            print('Crawled ' + str(len(all_items)) + ' items')

        self.all_items = all_items
        with open(self.file(), 'w', encoding='utf-8') as output_file:
            json.dump(all_items, output_file, ensure_ascii=False, indent=4)

    def url(self):
        return 'https://www.truefacet.com'

    def source(self):
        return 'Truefacet'

    def currency(self):
        return 'HKD'

    def file(self):
        return self.source() + '_' + self.object_type + '_' + self.category + '.json'
