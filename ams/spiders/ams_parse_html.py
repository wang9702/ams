# -*- coding: utf-8 -*-
import scrapy
import time, datetime
import pymongo
from bs4 import BeautifulSoup
from bs4 import Tag
from scrapy.http import HtmlResponse
import re

class ProcSpider(scrapy.Spider):
    name = 'ams_parse_html'
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
        self.publisher_journal_db = self.mongo_client['publisher_journal_db']
        self.publisher_journals_urls_col = self.publisher_journal_db['ams_journal_urls_col']

        self.mongo_client_1 = pymongo.MongoClient(host="117.119.77.139",
                                                  port=30019,
                                                  username="gong",
                                                  password="tipsikeg2012",
                                                  authSource="ysu",
                                                  authMechanism="SCRAM-SHA-1",
                                                  )
        self.db = self.mongo_client_1['ysu']
        self.collection = self.db['ams_journals_urls_col_crawling']
        self.collection.ensure_index('url', unique=True)

    def parse(self, response):
        for url_col in self.publisher_journals_urls_col.find({'ref': 0}).batch_size(2):
            id = url_col.get('_id')
            url = url_col.get('url')
            year = url_col.get('year')
            if url_col.get('html'):
                html = url_col['html']['html']
                response = HtmlResponse(url=url, body=bytes(html, encoding="utf-8"))
                yield self.parse1(response, id, year)

    def parse1(self, response, id, year):
        soup = BeautifulSoup(response.body, 'lxml')
        item = {}
        item['ts'] = datetime.datetime.strptime(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), '%Y-%m-%d %H:%M:%S')

        url = [response.url]
        item['url'] = url

        lang = 'en'
        item['lang'] = lang

        title = ''
        if ''.join(response.xpath('//p[@class="articleTitle"]/text()').extract()).strip():
            title = ''.join(response.xpath('//p[@class="articleTitle"]/text()').extract()).strip()
            item['title'] = {lang: self.format(title)}
        elif response.xpath('//html/head/meta[@name="citation_title"]/@content').extract_first():
            title = response.xpath('//meta[@name="citation_title"]/@content').extract_first()
            item['title'] = {lang: self.format(title)}
        if title:
            raw_title = response.xpath('//div[@class="col-xs-12 col-md-10 col-md-offset-1"]/p[@class="articleTitle"][2]').extract_first()
            item['raw_title'] = {lang: raw_title}

            words = item['title'][lang].split()
            hash = ''
            for word in words:
                if word[0]:
                    hash = hash + word[0].lower()

        issn = response.xpath('//meta[@name="citation_issn"]/@content').extract_first()
        item['issn'] = issn

        pdf_src = response.xpath('//meta[@name="citation_pdf_url"]/@content').extract()
        if pdf_src:
            item['pdf_src'] = pdf_src

        item['year'] = int(year)

        volume = response.xpath('//meta[@name="citation_volume"]/@content').extract_first()
        if volume:
            item['volume'] = volume
        else:
            item['volume'] = year

        issue = response.xpath('//meta[@name="citation_issue"]/@content').extract_first()
        if issue:
            item['issue'] = issue
        else:
            item['issue'] = '1'

        page_start = response.xpath('//meta[@name="citation_firstpage"]/@content').extract_first()
        page_end = response.xpath('//meta[@name="citation_lastpage"]/@content').extract_first()
        if page_start and page_end:
            item['page_start'] = page_start
            item['page_end'] = page_end
            item['page_str'] = page_start + '-' + page_end
        elif page_start:
            item['page_start'] = page_start
        elif page_end:
            item['page_end'] = page_end

        doi = response.xpath('//meta[@name="citation_doi"]/@content').extract_first()
        if doi:
            item['doi'] = doi
            sid = '/'.join(doi.split('/')[-2:])
            item['sid'] = sid
        else:
            item['sid'] = '/'.join(response.url.split('/')[-3, -1])

        src = 'ams'
        item['src'] = src

        keywords = response.xpath('//meta[@name="citation_keywords"]/@content').extract()
        if keywords:
            item['keywords'] = {lang: ';'.join(keywords)}

        date_str = response.xpath('//meta[@name="citation_online_date"]/@content').extract_first()
        if date_str:
            item['date_str'] = date_str

        venue = {}
        venue['name'] = {lang: response.xpath('//meta[@name="citation_journal_title"]/@content').extract_first()}
        venue['type'] = 1
        venue['sid'] = response.url.split('/')[4]
        item['venue'] = venue

        if response.xpath('//span[@class="freeJournalArticle"]/text()').extract_first() == 'Open Access':
            item['oa_type'] = 'open access'
        elif response.xpath('//span[@class="freeJournalArticle"]/text()').extract_first() == 'Free Access':
            item['oa_type'] = 'free'
        else:
            item['oa_type'] = 'none'

        author_list = soup.find_all('meta', attrs={'name': 'citation_author'})
        if author_list:
            authors = []
            for index in range(len(author_list)):
                author = {'pos': index,
                          'name': {lang: self.format(author_list[index]['content']).replace(',', '')}}
                org = []
                email_list = []
                for tag in author_list[index].next_siblings:
                    if isinstance(tag, Tag):
                        if 'name' in tag.attrs and tag['name'] == 'citation_author_institution':
                            org.append({lang: self.format(tag['content'])})
                        elif 'name' in tag.attrs and tag['name'] == 'citation_author_orcid':
                            author['orcid'] = tag['content']
                        elif 'name' in tag.attrs and tag['name'] == 'citation_author_email':
                            if re.findall(r'\(.*?\)', tag['content']):
                                email = tag['content'].replace(re.findall(r'\(.*?\)', tag['content'])[0], '')
                                email_list.append(self.format(email.replace(',', ';').replace(' and ', ';')))
                            else:
                                email_list.append(self.format(tag['content'].replace(',', ';').replace(' and ', ';')))
                        else:
                            break
                if len(email_list) != 0:
                    author['email'] = ';'.join(email_list)
                if len(org) != 0:
                    author['org'] = org
                elif response.xpath('//div[@class="wd-jnl-art-author-affiliations"]/p[@class="mb-05"]/text()').extract():
                    affi = response.xpath('//div[@class="wd-jnl-art-author-affiliations"]/p[@class="mb-05"]/text()').extract()
                    author['org'] = self.format(affi)
                authors.append(author)
            span_authors = soup.find_all('span', attrs={'itemprop': 'author'})
            for span_author in span_authors:
                name = span_author.find('span', attrs={'itemprop': 'name'}).get_text().strip()
                tag_orcid = span_author.find('a', attrs={'target': '_blank'})
                if tag_orcid:
                    for auth in authors:
                        if auth['name'] == name:
                            auth['orcid'] = tag_orcid.attrs['href']
            item['authors'] = authors

        if re.findall(r'(<a href="/jourhtml/abstracthelp.html#addinfo">Additional Notes:</a>[\s\S]*?)<br />', response.text, re.S | re.M):
            item['notes'] = [{lang: re.findall(r'(<a href="/jourhtml/abstracthelp.html#addinfo">Additional Notes:</a>[\s\S]*?)<a href="/jourhtml/abstracthelp.html#addinfo">Article copyright:</a>', response.text, re.S | re.M)[0]}]

        if response.xpath('//div[@class="col-xs-12 col-md-10 col-md-offset-1"]/p[@align="left"][2]/a/@name').extract_first() == 'Abstract':
            item['abstract'] = {lang: re.findall(r'(<a name="Abstract" id="Abstract" class="abstractSubHead">Abstract: </a>[\s\S]*?)<form action="">',response.text,re.S|re.M)[0]}

        reference_list = response.xpath('//div[@id="EnhancedReferences"]/span/ul/li').extract()
        if reference_list:
            references = []
            for reference in reference_list:
                references.append({lang: reference})
            item['reference'] = references
        self.save_to_mongo(dict(item), id)

    def format(self, s):
        return ' '.join(s.replace('\n', '').replace('\r', '').replace('\t', '').strip(',; ').split())

    def update_ref(self, id):
        try:
            if self.publisher_journals_urls_col.update_one({'_id': id}, {'$set': {'ref': 1}}):
                print('更新ref成功')
        except Exception as e:
            print(e)
            print("更新ref失败")

    def save_to_mongo(self, item, id):
        try:
            if self.collection.insert(item):
                self.update_ref(id)
                print('保存到MonGo成功')
        except Exception as e:
            print(e)



# span_list = re.findall(r'(<br />[\s\S]*?)<br />',response.text,re.M|re.S|re.I)
# for span in span_list:
#     if 'MSC' in span:
#         msc = span.replace('<span class="bibDataTag">MSC (2020):</span>','').replace('<br />','').strip()
# print(msc)



# info={}
# span_list=response.xpath('//span[@class="bibDataTag"]')
# for span in span_list:
#     if 'MathSciNet review:' in span.xpath('./text()').extract_first():
#         mathscinet_review = span.xpath('./following-sibling::a//text()').extract_first()
#         info['mathscinet_review'] = mathscinet_review.strip()
# text_list = response.xpath('//p[@align="left"]//text()').extract()
# for text in text_list:
#     if 'Primary' in text:
#         info['msc'] = text.strip()
# if info:
#     item['info'] = info