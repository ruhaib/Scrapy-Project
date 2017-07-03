import scrapy


class MarcJacobProduct(scrapy.Item):
    product_id = scrapy.Field()
    product_name = scrapy.Field()
    product_category = scrapy.Field()
    source_url = scrapy.Field()
    images = scrapy.Field()
    skus = scrapy.Field()



