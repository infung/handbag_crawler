import json
import os
import re
import socket
import ssl
import time
from abc import ABC, abstractmethod
from datetime import date

import requests
import undetected_chromedriver as uc
import urllib3
import urllib.request
from google.cloud import storage, firestore
from google.cloud.exceptions import GoogleCloudError, NotFound
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options

from crawl import SPECIAL_CHAR, month_year_string, BAG_MAPPING, BAG_DETAILS


class Crawler(ABC):
    def __init__(self, category, object_type):
        self.date = None
        self.object_type = object_type
        self.all_items = []
        self.ref_list = []
        self.model_list = []
        self.category = category

        self.year_string = month_year_string()

        # client = storage.Client.from_service_account_json(os.path.abspath('./credential/gcp-storage-key.json'))
        # self.bucket = client.get_bucket('training_image')


    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def source(self):
        pass

    @abstractmethod
    def url(self):
        pass

    @abstractmethod
    def currency(self):
        pass

    @abstractmethod
    def file(self):
        pass

    @staticmethod
    def create_folder_if_not_exists(item):
        if item['folder'] != '' and not os.path.exists(item['folder']):
            os.makedirs(item['folder'])

    @staticmethod
    def crawl_image(driver, path, image):
        if not os.path.exists(path):
            print('downloading: ' + image)

            try:
                print(image)
                driver.get(image)
                time.sleep(2)
                userAgent = driver.execute_script("return navigator.userAgent;")
                seleniumCookies = driver.get_cookies()
                cookies = ''
                for cookie in seleniumCookies:
                    cookies += '%s=%s;' % (cookie['name'], cookie['value'])

                opener = urllib.request.build_opener()
                opener.addheaders = [('User-Agent', userAgent)]
                opener.addheaders.append(('Cookie', cookies))

                urllib.request.urlretrieve(image, path)
                print('downloaded to ' + path)
                return True
            except TimeoutException as e:
                print('Failed to download ' + image + ': ' + e.msg)
        else:
            print('item already downloaded: ' + path)
        return False

    @staticmethod
    def save_image(driver, path, image):
        if not os.path.exists(path):
            print('downloading: ' + image)

            try:
                driver.get(image)
                driver.save_screenshot(path)

                print('downloaded to ' + path)
                return True
            except TimeoutException as e:
                print('Failed to download ' + image + ': ' + e.msg)
        else:
            print('item already downloaded: ' + path)

        return False

    @staticmethod
    def download_image(item, path, image):
        if not os.path.exists(path):
            count = 4

            try:
                while count >= 0:
                    download_path = image

                    if item['source'] == 'Truefacet' \
                            and 'https://media.truefacet.com/media/catalog/productno_selection' in item['image']:
                        print(item)
                        return False

                    print('downloading: ' + download_path)
                    response = requests.get(download_path, timeout=10, headers={
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
                    })
                    if response.status_code == 200:
                        print('downloaded')
                        img_data = response.content

                        with open(path, 'wb') as handler:
                            handler.write(img_data)

                        return True
                    elif response.status_code == 403:
                        return False
                    elif response.status_code == 404:
                        return False
                    else:
                        print('unknown status code: ' + str(response.status_code) + ', response: ' + str(
                            response.content))
            except (requests.exceptions.InvalidSchema, requests.exceptions.MissingSchema, requests.exceptions.Timeout,
                    requests.exceptions.SSLError, ssl.SSLError, socket.timeout, urllib3.exceptions.ReadTimeoutError,
                    requests.exceptions.ConnectionError):
                print(item['id'])
        else:
            print('item already downloaded: ' + path)

        return False

    @staticmethod
    def generate_keywords(title):
        keyword_set = set()

        words = title.split(' ')
        for word in words:
            if word != '':
                sanitized_word = ''.join([c for c in word if c.isalnum()])
                if sanitized_word != '':
                    keyword_set.add(sanitized_word.lower())

        mapping = {}
        for keyword in keyword_set:
            mapping[keyword] = True

        return mapping

    def is_valid_title(self, title):
        regex = r'^[A-Za-z0-9\u00C0-\u00ff]+$'
        parts = title.split(' ')
        for part in parts:
            if len(part) > 1 and not re.match(regex, part):
                return False

        return True

    def get_folder(self, title, model=''):
        return 'bag/' + self.category.lower() + '/' + model

    def upload_image(self, item):
        # file = item['folder'] + '/' + str(item['id']) + '.jpg'
        # path = './' + file
        #
        # blob = self.bucket.blob(file)
        # blob.upload_from_filename(path, timeout=30)
        # print('uploaded: ' + file)
        #
        # with open(self.__get_crawl_id_file(), 'a+') as f:
        #     f.write(str(item['id']) + '\n')
        return


    def get_bag_model(self, title):
        model = None
        keys = BAG_MAPPING[self.category.lower()].keys()
        for key in keys:
            if key in title.lower():
                model = key
                break
        return model

    def get_bag_collection(self, title):
        collection = None
        keys = BAG_DETAILS['collection'][self.category.lower()]
        for key in keys:
            if key in title.lower():
                collection = key
                break
        return collection

    def get_bag_size(self, title, collection):
        size = None
        sizes = BAG_MAPPING[self.category.lower()][collection]
        for s in sizes:
            if s in title.lower():
                size = s
                break
        return size

    def get_bag_detail(self, title, name):
        detail = None
        details = BAG_DETAILS[name]
        for d in details:
            if d in title.lower():
                detail = d
                break
        return detail

        # return ''

    def __get_crawl_id_file(self):
        return 'crawl_' + self.object_type + '_id'

    def __save_trend_records(self, db, brand, item):
        trend_col = 'BagPriceTrend'
        try:
            db.collection(trend_col).document(item['id']).update(
                {'trends': firestore.ArrayUnion(item['trends']),
                 'price': item['price'],
                 'brand': brand,
                 'category': self.object_type})
        except NotFound:
            try:
                item['brand'] = brand
                item['category'] = self.object_type
                db.collection(trend_col).document(item['id']).set(item)
            except ValueError as e:
                print(item)
                print('failed to insert trend:' + str(e))

    def __save_records(self, db, brand, item):
        try:
            db.collection(brand).document(item['id']).update(
                {'trends': firestore.ArrayUnion(item['trends']),
                 'price': item['price'],
                 'brand': brand,
                 'category': self.object_type})
        except NotFound:
            try:
                print('new item: ' + item['id'] + ', source:' + item['source'])
                db.collection(brand).document(item['id']).set(item)
            except ValueError as e:
                print(item)
                print('failed to insert:' + str(e))

    def __download_and_upload_image(self, driver):
        retry_list = []

        with open(self.file(), encoding='utf-8') as input_file:
            items = json.load(input_file)

        for item in items:
            item_id = item['id']
            # if item_id not in content:
            Crawler.create_folder_if_not_exists(item)

            if item['folder'] != '':
                file = item['folder'] + '/' + str(item_id) + '.jpg'
                path = './' + file
                if driver is not None:
                    if item['source'] == 'Vestiaire Collective':
                        result = Crawler.crawl_image(driver, path, item['image'])
                    else:
                        result = Crawler.save_image(driver, path, item['image'])
                else:
                    result = Crawler.download_image(item, path, item['image'])
                if result:
                    try:
                        self.upload_image(item)
                    except (GoogleCloudError, requests.exceptions.Timeout, ConnectionResetError):
                        print('upload failed: ' + file)
                        retry_list.append(item)

        return retry_list

    def sanitize_title(self, input_title):
        title = input_title \
            .replace('&', ' ').replace("'", '') \
            .replace('"', '').replace('+', '') \
            .replace('”', '').replace('“', '') \
            .replace(',', ' ').replace('|', '') \
            .replace('(', '').replace(')', '') \
            .replace('[', '').replace(']', '') \
            .replace(':', '').replace('_', '') \
            .replace('‘', '').replace('#', '') \
            .replace('’', '').replace('„', '') \
            .replace('%', '').replace('´', '') \
            .replace('\\', '').replace('’s', '') \
            .replace(u'\u3000', ' ').replace(u'\ufeff', ' ')

        for char in SPECIAL_CHAR:
            title = title.replace(char, '')

        title = ' '.join(title.split())
        return title

    def save_records(self, db, items=None):
        brand = self.category.lower().replace(' ', '_')

        if items is None:
            with open(self.file(), encoding='utf-8') as input_file:
                items = json.load(input_file)

        for item in items:
            self.__save_records(db, brand, item)
            self.__save_trend_records(db, brand, item)

    def get_image(self, driver=None):
        retry_list = self.__download_and_upload_image(driver)
        while len(retry_list) > 0:
            print('num of item to retry: ' + str(len(retry_list)))

            temp_list = retry_list[:]
            for i in range(len(retry_list)):
                try:
                    self.upload_image(retry_list[i])
                    temp_list.pop(i)
                except (GoogleCloudError, requests.exceptions.Timeout):
                    print(retry_list[i]['id'])

            retry_list = temp_list[:]

    def get_date(self):
        return str(date.today()) if self.date is None else self.date


class WebDriver:
    SCROLL_TO_BOTTOM = '''
function scrollTo(element, duration) {
  let e = document.documentElement;
  if (e.scrollTop === 0) {
    const t = e.scrollTop;
    ++e.scrollTop;
    e = t + 1 === e.scrollTop-- ? e : document.body;
  }
  scrollToC(e, e.scrollTop, element, duration);
}

// Element to move, element or px from, element or px to, time in ms to animate
function scrollToC(element, from, to, duration) {
  if (duration <= 0) return;
  if (typeof from === "object") from = from.offsetTop;
  if (typeof to === "object") to = to.offsetTop;

  scrollToX(element, from, to, 0, 1 / duration, 20, easeOutCuaic);
}

function scrollToX(element, xFrom, xTo, t01, speed, step, motion) {
  if (t01 < 0 || t01 > 1 || speed <= 0) {
    element.scrollTop = xTo;
    return;
  }
  element.scrollTop = xFrom - (xFrom - xTo) * motion(t01);
  t01 += speed * step;

  setTimeout(function () {
    scrollToX(element, xFrom, xTo, t01, speed, step, motion);
  }, step);
}

function easeOutCuaic(t) {
  t--;
  return t * t * t + 1;
}
scrollTo(document.body.scrollHeight, 5000);
    '''

    @staticmethod
    def chrome(headless=False):
        # No UI rendering, best for memory and performance
        options = Options()
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--allow-running-insecure-content')
        options.add_argument('--disable-blink-features=AutomationControlled')
        if headless:
            options.add_argument('--headless')

        driver = uc.Chrome(options=options)

        return driver
