import re

from datetime import datetime
from scrapy import FormRequest, Request
from scrapy.spiders import CrawlSpider
from scrapy.selector import XPathSelector


def _sanitize(input_val):
    if not isinstance(input_val, str) and getattr(input_val, '__iter__', False):
        return clean(input_val)

    if isinstance(input_val, XPathSelector):
        to_clean = input_val.extract()
    else:
        to_clean = input_val

    return re.sub('\s+', ' ', to_clean.replace('\xa0', ' ')).strip()


def clean(lst_or_str):
    if not isinstance(lst_or_str, str) and getattr(lst_or_str, '__iter__', False):  # if iterable and not a string like
        return [x for x in (_sanitize(y) for y in lst_or_str if y is not None) if x]
    return _sanitize(lst_or_str)


class MixinBoerseFrankFurt:
    exchange = 'boersefrankfurt'
    allowed_domains = ['en.boerse-frankfurt.de']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'FEED_FORMAT':  'jsonlines',
        'FEED_URI': 'boersefrankfurt.jsonlines'
    }

    start_urls = [
        'http://en.boerse-frankfurt.de/etp/iShares-S&P-500-UCITS-ETF-Dist-IE0031442068/FSE',
        'http://en.boerse-frankfurt.de/etp/Multi-Units-Lux----Lyxor-ETF-S&P-500-D-EUR-LU0496786574/FSE',
        'http://en.boerse-frankfurt.de/etp/Lyxor-EURO-STOXX-50-DR---UCITS-ETF-C-EUR-LU0908501215',
        'http://en.boerse-frankfurt.de/etp/Amundi-CAC-40-UCITS-ETF-DR-EUR-C-Acc-LU1681046931/FSE',
        'http://en.boerse-frankfurt.de/etp/Commerzbank-CCBI-RQFII-Money-Market-UCITS-ETF-A-RMB-GB00BVJF7G73/ITF',
        'http://en.boerse-frankfurt.de/etp/BNPP-Kupfer-ETC-DE000PB8C0P8/ETR',
        'http://en.boerse-frankfurt.de/etp/iShares-OMX-Stockholm-Capped-UCITS-ETF-IE00BD3RYZ16/ITF',
    ]


class BoerseFrankfurtSpider(MixinBoerseFrankFurt, CrawlSpider):
    name = MixinBoerseFrankFurt.exchange
    # download_delay = 0.25

    def parse_start_url(self, response):
        sub_line = response.css('.stock-subline h1::text').extract_first()
        sub_line_info = response.xpath('normalize-space(//*[@class="stock-subline"])').extract_first()

        item = {
            'name': response.css('.stock-headline::text').extract_first(),
            sub_line: sub_line_info,
            'Benchmark': self.get_table_data(response, 'Benchmark'),
            'Trading Parameters': self.get_table_data(response, 'Trading Parameters'),
            'Fees': self.get_table_data(response, 'Fees'),
            'Liquidity': self.get_table_data(response, 'Liquidity'),
            'Issuer': self.get_table_data(response, 'Issuer'),
            'Master Data': self.get_table_data(response, 'Master Data'),
            'Dates': self.get_table_data(response, 'Dates'),
            'Price Information': self.get_table_data(response, 'Price Information'),
            'Price History': self.get_table_data(response, 'Price History'),
        }

        return response.follow(response.css('[name="History"]')[0], meta={'item': item}, callback=self.parse_history)

    def next_request_or_item(self, item):
        if item['meta'].get('requests_queue', []):
            request = item['meta']['requests_queue'].pop()
            request.meta.update({'item': item})
            return request
        elif 'requests_queue' in item['meta']:
            item.pop('meta')

        return item

    def parse_history(self, response):
        item = response.meta['item']
        item['Historic Bid Ask Price History'] = {}
        item['meta'] = {
            'requests_queue': self.history_requests(response, item)
        }
        return self.next_request_or_item(item)

    def history_requests(self, response, item):
        xpath = '(//a[(contains(., "Frankfurt") or contains(., "Xetra")) and span]/@href)[1]'
        end_date = datetime.now().strftime('%d.%m.%Y')
        name_id = response.xpath(xpath).extract_first().replace('/etp/', '')
        url = f'http://en.boerse-frankfurt.de/Ajax/ETPController_HistoricPriceList/{name_id}/1.1.2014_{end_date}'
        requests = [Request(url, method='POST', callback=self.parse_historical_data)]

        item['ISIN'] = re.findall('-(?!.*-)(.*?)/(?!.*/).*?$', name_id)[0].upper()
        url_part = re.findall('-(?!.*-)(.*)', name_id)[0].upper()
        url = f'http://en.boerse-frankfurt.de/ajax/ETPController_HistoricBidAskPriceList/{url_part}'
        dates = response.xpath('//*[@name="date"]//@value').extract()
        bid_ask_req = FormRequest(url, formdata={'d': ''}, callback=self.parse_historic_bid_ask_pricelist)
        for date in dates:
            requests += [bid_ask_req.replace(formdata={
                'd': f"{date} 00:00-{date} 23:59"
            })]
        return requests

    def parse_historic_bid_ask_pricelist(self, response):
        item = response.meta['item']
        for_date = re.findall('=(.*?)\+', response.request.body.decode())[0]
        item['Historic Bid Ask Price History'].update({for_date: self.get_bid_ask_pricelist(response)})
        return self.next_request_or_item(item)

    def get_bid_ask_pricelist(self, response):
        if response.xpath('//strong[contains(., "No results")]'):
            return [{'Time': 'No results'}]

        heads = response.css('thead th')
        heads = [h.xpath('normalize-space(.//parent::th)').extract_first() for h in heads]

        body = response.css('.table > tr')
        data = []
        for row in body:
            row_tds = row.css('td')
            data_tds = [td.xpath('normalize-space(.//parent::td)').extract_first() for td in row_tds]
            data += [{k: v for k, v in zip(heads, data_tds)}]
        return data

    def parse_historical_data(self, response):
        item = response.meta['item']
        item['Price History'] = self.get_price_history(response)
        return self.next_request_or_item(item)

    def get_price_history(self, response):
        price_history = []
        head_s = response.xpath('//*[@class="table"]//th')
        heads = [h.xpath('normalize-space(.//span)').extract_first() for h in head_s]

        for data_row in response.xpath('//tbody//tr'):
            data = [d.xpath('normalize-space(./text())').extract_first() for d in data_row.xpath('.//td')]
            price_history += [{h: v for h, v in zip(heads, data)}]

        return price_history

    def get_table_data(self, response, head):
        data_s = response.xpath(f'//h2[.="{head}"]/ancestor::*[@class="box"]//td')

        if not data_s:
            return {}

        if head == 'Price Information':
            data = clean([''.join(clean(td.xpath('.//text()[not(ancestor::td[contains(@class, "hidden")])]')))
                          for td in data_s])
        else:
            data = [x.xpath('normalize-space(./text())').extract_first() or x.xpath('./a/@href').extract_first('')
                    for x in data_s]

        return {k: v for k, v in zip(data[0::2], data[1::2])}
