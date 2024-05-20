import json
from abc import ABC

import requests

from crawl.Crawler import Crawler as AbstractCrawler
from unidecode import unidecode

mapping = {
    'Balenciaga': ['187'],
    'Chanel': ['50'],
    'Fendi': ['57'],
    'Gucci': ['2'],
    'Hermes': ['14'],
    'Louis Vuitton': ['17'],
    'Prada': ['60'],
    'Saint Laurent': ['3119'],
    'Celine': ['6'],
    'Cartier': ['434'],
    'Dior': ['10'],
    'Valentino Garavani': ['14193', '88'],
    'Loewe': ['433'],
    'Bottega Veneta': ['115'],
    'Givenchy': ['66'],
    'Goyard': ['192'],
    'Miu Miu': ['117'],
    'Bvlgari': ['725'],
    'Chloe': ['9'],

}


class Crawler(AbstractCrawler, ABC):
    def __init__(self, category='Chanel', object_type='bag'):
        super().__init__(category, object_type)

        self.crawlUrl = 'https://search.vestiairecollective.com/v1/product/search'
        self.category = category
        self.object_type = object_type

    def __crawl(self):
        page = 0
        count = 0
        limit = 200
        items = []

        while True:
            data = {
                'pagination': {
                    'offset': page * limit, 'limit': limit
                },
                'fields': ['name', 'description', 'brand', 'model', 'country', 'price', 'discount', 'link', 'sold',
                           'likes', 'editorPicks', 'shouldBeGone', 'seller', 'directShipping', 'local', 'pictures',
                           'colors', 'size', 'stock', 'universeId'
                           ],
                'facets': {'fields': [], 'stats': []},
                'q': None,
                'sortBy': 'relevance',
                'filters': {
                    'brand.id': mapping[self.category],
                    'universe.id': ['1'],
                    'catalogLinksWithoutLanguage': ['/women-bags/' + self.category.lower() + '/']
                },
                'locale': {'country': 'HK', 'currency': 'HKD', 'language': 'en', 'sizeType': 'US'},
                'mySizes': None
            }
            r = requests.post(url=self.crawlUrl, json=data, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0'})
            rsp = r.json()
            if 'items' not in rsp:
                break

            print(self.crawlUrl + '/' + self.category)

            result_list = rsp['items']
            for item in result_list:
                try:
                    title = self.category + ' ' + unidecode(item['name'])
                    title = self.sanitize_title(title)
                    if not self.is_valid_title(title):
                        continue

                    condition = 'Pre-Owned'
                    description = item['description'].lower()
                    if 'never used' in description or 'never worn' in description:
                        condition = 'New'
                    elif 'excellent' in description and 'condition' in description:
                        condition = 'very good condition'
                    elif 'very good' in description and 'condition' in description:
                        condition = 'very good condition'
                    elif 'good' in description and 'condition' in description:
                        condition = 'good condition'
                    elif 'fair' in description and 'condition' in description:
                        condition = 'fair condition'
                    elif 'new' in description and 'condition' in description:
                        condition = 'New'

                    product_id = str(item['id'])
                    price = item['price']['cents'] / 100
                    like = item['likes']

                    model = self.get_bag_collection(title)
                    color = self.get_bag_detail(title, 'color')
                    cat = self.get_bag_detail(title, 'category')
                    material = self.get_bag_detail(title, 'material')
                    if color is None and len(item['colors']['all']) > 0:
                        color = item['colors']['all'][0]['name'].lower()
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
                        'url': self.url() + item['link'],
                        'id': self.source().lower() + '-' + product_id,
                        'productId': product_id,
                        'image': 'https://images.vestiairecollective.com/cdn-cgi/image/w=1024,h=1024,q=100,f=auto,' +
                                 item['pictures'][0],
                        'folder': self.get_folder(title, model),
                        'currency': self.currency(),
                        'condition': condition,
                        # 'keywords': AbstractCrawler.generate_keywords(title),
                        'size': size
                    })
                    count += 1
                except KeyError as e:
                    print(item)
                    print(e)

            page += 1

        return items

    def start(self):
        self.all_items = self.__crawl()
        print('Crawled ' + str(len(self.all_items)) + ' items')

        with open(self.file(), 'w', encoding='utf-8') as output_file:
            json.dump(self.all_items, output_file, ensure_ascii=False, indent=4)

    def url(self):
        return 'https://us.vestiairecollective.com'

    def source(self):
        return 'Vestiaire Collective'

    def currency(self):
        return 'HKD'

    def file(self):
        return self.source() + '_' + self.object_type + '_' + self.category + '.json'
