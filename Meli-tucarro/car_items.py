
import re
import scrapy
import logging
import requests
import unidecode
from datetime import datetime
from Carros.items import MlibreItem
from scrapy.http import TextResponse
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging


class CarrosmlSpider(scrapy.Spider):

    name = 'carros'
    allowed_domains = ['tucarro.com.co']

    # Modelos de Carros en Bogota desde Modelo 2005 en adelante
    r = requests.get('https://carros.tucarro.com.co/bogota-dc/desde-2005/_DisplayType_LF')

    response = TextResponse(r.url, body=r.text, encoding='utf-8')
    results = response.xpath(
        "//*[@class='modal-overlay modal-location-filter modal-location-filter-9991744-AMCO_1744_2']//*"
        "[@class='location_filter_inner']/a/@href").extract()

    start_urls = results
    custom_settings = {
        'FEED_URI': 'carros_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORTERS': {
            'json': 'scrapy.exporters.CsvItemExporter',
        },
        'FEED_EXPORT_ENCODING': 'utf-8',
        'SPIDER_MIDDLEWARES': {
                    'scrapy_deltafetch.DeltaFetch': 500,
                    'Carros.middlewares.CarrosSpiderMiddleware': 400,
                    'scrapy_magicfields.MagicFieldsMiddleware': 400,
            },
        'MAGIC_FIELDS': {
            "link": "$response:url",
            "fecha_scraping": "$time",
            },
        'MAGICFIELDS_ENABLED': True,
        'DELTAFETCH_ENABLED': True,
        'DELTAFETCH_RESET': 1,
        'EXTENSIONS': {
            'scrapy.extensions.statsmailer.StatsMailer': 500,
        },
        'STATSMAILER_RCPTS': ['ajimenezbonilla@gmail.com'],
        'MAIL_HOST': 'smtp.gmail.com',
        'MAIL_PORT': 587,
        'MAIL_USER': 'pruebacvmobile@gmail.com',
        'MAIL_PASS': 'Loteamo2015'
    }

    def parse(self, response):

        product_links = response.css('div[class="image-content"] a::attr(href)').extract()
        for i in product_links:
            url = response.urljoin(i)
            yield response.follow(url, callback=self.get_details)

        next_page = response.xpath(
            "//li[@class='andes-pagination__button andes-pagination__button--next ']/a/@href").extract_first()
        if next_page:
            next_page = response.urljoin(next_page)
            yield response.follow(next_page, callback=self.parse)

    def get_details(self, response):

        def getint(value):
            try:
                return int(value.replace('.',''))
            except Exception:
                return int(value)

        def modelo():
            try:
                s = response.xpath("//ul[@class='specs-list']/li/span/text()").extract()[1]
                return noaccent(s)
            except Exception:
                return noaccent(response.xpath(
                    "normalize-space(.//section[@class='ui-view-more vip-section-specs main-section'])"
                    ).re_first(r'Modelo (\S+)'))

        def noaccent(s):
            try:
                return str(unidecode.unidecode(s))
            except Exception:
                return None

        def info(s):
            def tostr(s):
                return ' '.join(s)
            def accent(s):
                return str(unidecode.unidecode(s))
            return accent(tostr(s))

        items = MlibreItem()

        items['id'] = response.css('.item-info__id-number::text').re_first(r'\d+')

        items['producto'] = noaccent(response.xpath(
            "normalize-space(.//h1[@class='item-title__primary ']/text())").extract_first())

        items['marca'] = noaccent(response.xpath(
            "normalize-space(.//ul[@class='specs-list']/li/span/text())").extract_first())

        items['modelo'] = modelo()

        items['version'] = noaccent(response.xpath(
            "//ul[@class='specs-list']/li/span/text()").extract()[2])

        items['precio'] = getint(response.xpath(
            "//span[@class='price-tag-fraction']/text()").extract_first())

        items['km'] = getint(response.xpath(
            "//div[@class='short-description__floating']/article/dl/dd/text()").re(r'[^ km]+')[1])

        items['year'] = response.xpath(
            "//div[@class='short-description__floating']/article/dl/dd/text()").re_first(r'\d+')

        items['puertas'] = response.xpath(
            "normalize-space(.//section[@class='ui-view-more vip-section-specs main-section'])"
            ).re_first(r'Puertas (\S+)')

        items['transmision'] = noaccent(response.xpath(
            "normalize-space(.//section[@class='ui-view-more vip-section-specs main-section'])"
            ).re_first(r'Transmisión : (\S+)'))

        items['direccion'] = noaccent(response.xpath(
            "normalize-space(.//section[@class='ui-view-more vip-section-specs main-section'])"
            ).re_first(r'Dirección : (\S+)'))

        items['placa'] = noaccent(response.xpath(
            "normalize-space(.//section[@class='ui-view-more vip-section-specs main-section'])"
            ).re_first(r'Placa (\S+)'))

        items['color'] = response.xpath(
            "normalize-space(.//section[@class='ui-view-more vip-section-specs main-section'])"
            ).re_first(r'Color : (\S+)')

        items['vendedor'] = noaccent(response.xpath(
            "normalize-space(.//p[@class='card-description card-description--bold']/span/text())").extract_first())

        items['tel_contacto'] = response.xpath(
            "normalize-space(.//span[@class='profile-info-phone-value']/text())").extract_first()

        items['ubicacion'] = noaccent(response.xpath(
            "//div[@class='location-info']/span/text()").re_first(r'^(?:\S+\s){4}(\D+)'))

        items['info_adicional'] = noaccent(response.xpath(
            "normalize-space(.//section[@class='ui-view-more vip-section-specs main-section'])").re_first(
            r'^\w+\s+\w+\s+(.*)'))

        items['descripcion'] = info(response.xpath("//div[@class='item-description__text']/p/text()").extract())

        yield items


configure_logging()
runner = CrawlerRunner()
runner.crawl(CarrosmlSpider)
d = runner.join()
d.addBoth(lambda _: reactor.stop())

reactor.run()
