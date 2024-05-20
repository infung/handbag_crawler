from constant import bag_brands as brands
from crawl import FpCrawler, RebagCrawler, VcCrawler, TrueFacetCrawler, CsCrawler, Crawler


def crawl_rebag_data():
    for brand in brands:
        crawler = RebagCrawler.Crawler(category=brand, object_type='bag')
        # crawler.start()
        crawler.get_image()


def crawl_vc_data():
    # chrome_driver = Crawler.WebDriver().chrome()
    for brand in brands:
        vc_crawler = VcCrawler.Crawler(category=brand, object_type='bag')
        vc_crawler.start()
    #     vc_crawler.get_image(driver=chrome_driver)
    #
    # chrome_driver.close()


def crawl_tf_data():
    for brand in brands:
        tf_crawler = TrueFacetCrawler.Crawler(category=brand, object_type='bag')
        # tf_crawler.start()
        tf_crawler.get_image()


def crawl_fp_data():
    for brand in brands:
        fp_crawler = FpCrawler.Crawler(category=brand, object_type='bag')
        # fp_crawler.start()
        fp_crawler.get_image()


def crawl_cs_data():
    for brand in brands:
        cs_crawler = CsCrawler.Crawler(category=brand, object_type='bag')
        # cs_crawler.start()
        cs_crawler.get_image()


if __name__ == '__main__':
    # crawl_cs_data()
    crawl_tf_data()
    # crawl_vc_data()
    crawl_rebag_data()
    crawl_fp_data()
