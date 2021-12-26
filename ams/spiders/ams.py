import scrapy
import pymongo
import re

class AmsSpider(scrapy.Spider):
    name = 'ams'
    allowed_domains = ['ams.org']
    start_urls = ['https://www.ams.org/journals']
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
    }
    def __init__(self):
        self.mongo_client = pymongo.MongoClient(host="10.200.0.84",
                                                port=27017,
                                                username="mongouser",
                                                password="YsuKeg0225",
                                                authSource="admin",
                                                authMechanism="SCRAM-SHA-1",
                                                )
        # self.mongo_client = pymongo.MongoClient('mongodb://localhost:27017')
        self.publisher_journal_db = self.mongo_client['publisher_journal_db']
        self.publisher_journals_urls_col = self.publisher_journal_db['ams_journal_urls_col']
        self.publisher_journals_urls_col.ensure_index('url', unique=True)
    def parse(self,response):
        journal_list = response.xpath('//div[@id="research"]/div/blockquote/p/a[1]/@href').extract()
        journal_list.append(response.xpath('//div[@id="research"]/div/blockquote/p/em/a/@href').extract_first())
        journal_list.append(response.xpath('//div[@id="research"]/div/blockquote/div/div/a/@href').extract_first())
        for journal in journal_list:
            url = response.urljoin(journal)
            yield scrapy.Request(
                url=url,
                callback=self.parse_journal,
                headers=self.header
            )
    def parse_journal(self, response):
        url = response.xpath('//div[@id="nav_cgi"]/a[last()]/@href').extract_first()
        yield scrapy.Request(
            url=response.urljoin(url),
            callback=self.parse_volume,
            headers=self.header
        )

    def parse_volume(self, response):
        issue_list = re.findall(r'<a href="(/journals/\w+/.*?)">.*?</a>', response.text)
        for issue in issue_list:
            url = response.urljoin(issue)
            year_volume_issue = issue.split('/')[-1]
            if year_volume_issue == '':
                year_volume_issue = issue.split('/')[-2]
            if len(year_volume_issue.split('-')) == 3:
                year = year_volume_issue.split('-')[0]
                # vol = year_volume_issue[1]
            else:
                year = year_volume_issue.split('-')[1].split('.')[0]

            yield scrapy.Request(
                url=url,
                headers=self.header,
                callback=self.parse_issue,
                meta={'year': year}
            )
    def parse_issue(self, response):
        article_list = re.findall('<a href="(.*?)">                Abstract, references and article information        </a>', response.text)
        for article in article_list:
            url = response.urljoin(article)
            yield scrapy.Request(
                url=url,
                callback=self.parse_article,
                headers=self.header,
                meta=response.meta
            )
    def parse_article(self, response):
        item={}
        item['url'] = response.url
        item['year'] = response.meta['year']
        item['volume'] = response.xpath('//meta[@name="citation_volume"]/@content').extract_first()
        item['issue'] = response.xpath('//meta[@name="citation_issue"]/@content').extract_first()
        # item['issn'] = '1088-4165'
        item['ref'] = 0
        item['jconf_id'] = response.url.split('/')[4]
        item['html'] = {'html': response.text}
        self.save_to_mongo(item)

    def save_to_mongo(self, item):
        try:
            if self.publisher_journals_urls_col.insert(item):
                print('保存到MonGo成功')
        except Exception as e:
            print(e)
            print('存储到MonGo失败')