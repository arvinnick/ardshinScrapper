from pathlib import Path

import scrapy


class RatesSpider(scrapy.Spider):
    name = "rate"

    css_selector_format = "#cash > table:nth-child(1) > tbody:nth-child(1) > tr > td:nth-child({}) > span:nth-child(1)::text"
    pair_css_selector = css_selector_format.format(1)
    buy_css_selector = css_selector_format.format(2)
    sell_css_selector = css_selector_format.format(3)

    start_urls = [
        'https://www.ardshinbank.am/en'
    ]

    def parse(self, response):
        pairs = response.css(self.pair_css_selector).getall()
        buy_rates = response.css(self.buy_css_selector).getall()
        sell_rates = response.css(self.sell_css_selector).getall()
        for pair, buy_rate, sell_rate in zip(pairs, buy_rates, sell_rates):
            filename = f'rate-{pair}.txt'
            Path(filename).write_text(pair + "," + buy_rate + "," + sell_rate)
            self.log(f'Saved file {filename}')
