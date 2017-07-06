# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from super_store.models import Brand, Product, Images, Skus


class MarJacobProductPipeline(object):
    def process_item(self, item, spider):
        brand = Brand.objects.get(name="MarcJacobs")
        product = Product()
        product.brand = brand
        product.product_id = item['product_id']
        product.product_name = item['product_name']
        product.category = item['product_category']
        product.source_url = item['source_url']
        product.save()

        images = []
        for img in item['images']:
            images.append(Images(product=product, image_url=img))
        Images.objects.bulk_create(images)

        skus = []
        for sku in item['skus']:
            skus.append(Skus(
                product=product,
                color=sku['color'],
                availability=sku['availability'],
                price=sku['price'],
                size=sku['size']))
        Skus.objects.bulk_create(skus)
        return item
