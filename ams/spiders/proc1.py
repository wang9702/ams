# -*- coding: utf-8 -*-
import scrapy
import time,datetime
import pymongo
from bs4 import BeautifulSoup
from bs4 import Tag
class ProcSpider(scrapy.Spider):
    name = 'proc1'
    allowed_domains = ['ams.org']
    start_urls = ['http://ams.org/']
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
    }
    def __init__(self):
        self.client = pymongo.MongoClient(host='localhost', port=27017)
        # self.db_auth = self.client.admin
        # self.db_auth.authenticate(name="Keger", password="YsuKeg@2017gong", mechanism="MONGODB-CR")
        self.db = self.client['YanTask']
        self.MONGO_TABLE = 'PROCEEDINGS_OF_THE_AMERICAN_MATHEMATICAL_SOCIETY'
    def start_requests(self):
        for year in range(1969,1995):
            for volume in range(20,123):
                if volume < 100:
                    vol = '0' + str(volume)
                else:
                    vol =str(volume)
                for issue in range(1,13):
                    if issue<10:
                        iss='0'+str(issue)
                    else:
                        iss=str(issue)
                    url='https://www.ams.org/journals/proc/'+str(year)+'-'+vol+'-'+iss+'/home.html'
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse,
                        headers=self.header,
                        meta={'year':year,'volume':vol,'issue':iss}
                    )
    def parse(self, response):
        year=response.meta['year']
        arg_vol=response.meta['volume']
        arg_iss=response.meta['issue']
        article_list=response.xpath('//div[@class="contentList"]/dl/dd/a[last()-3]/@href').extract()
        for article in article_list:
            url='https://www.ams.org/journals/proc/'+str(year)+'-'+arg_vol+'-'+arg_iss+'/'+article
            yield scrapy.Request(
                url=url,
                callback=self.parse_article,
                headers=self.header,
            )
    def parse_article(self,response):
        soup = BeautifulSoup(response.body, 'lxml')
        item = {}
        item['ts'] = datetime.datetime.strptime(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),'%Y-%m-%d %H:%M:%S')

        url = [response.url]
        item['url'] = url

        title=''
        if response.xpath('//html/head/meta[@name="citation_title"]/@content').extract_first():
            title = response.xpath('//html/head/meta[@name="citation_title"]/@content').extract_first()
            item['title'] = ' '.join(title.replace('\n', '').replace('\t', '').replace('\r', '').strip().split())
        elif ''.join(response.xpath('//p[@class="articleTitle"]/text()').extract()).strip():
            title = ''.join(response.xpath('//p[@class="articleTitle"]/text()').extract()).strip()
            item['title'] = ' '.join(title.replace('\n', '').replace('\t', '').replace('\r', '').strip().split())
        if title:
            raw_title=response.xpath('//div[@class="col-xs-12 col-md-10 col-md-offset-1"]/p[@class="articleTitle"][2]').extract_first()
            item['raw_title']=raw_title

            words = item['title'].split()
            new_words = []
            for word in words:
                if word:
                    new_words.append(word)
            hash = ''
            for word in new_words:
                if word[0]:
                    hash = hash + word[0].lower()
            item['hash'] = hash
        lang=response.xpath('//html/@lang').extract_first()
        item['lang']=lang

        issn=response.xpath('//html/head/meta[@name="citation_issn"]/@content').extract_first()
        item['issn']=issn

        pdf_src=response.xpath('//html/head/meta[@name="citation_pdf_url"]/@content').extract()
        if pdf_src:
            item['pdf_src'] = pdf_src

        volume=response.xpath('//html/head/meta[@name="citation_volume"]/@content').extract_first()
        item['volume']=volume

        issue=response.xpath('//html/head/meta[@name="citation_issue"]/@content').extract_first()
        item['issue']=issue

        page_start = response.xpath('//head/meta[@name="citation_firstpage"]/@content').extract_first()
        page_end = response.xpath('//head/meta[@name="citation_lastpage"]/@content').extract_first()
        if page_start:
            item['page_start'] = page_start
        elif page_end:
            item['page_start'] = page_end
        if page_end:
            item['page_end'] = page_end
        elif page_start:
            item['page_end'] = page_start
        if page_start and page_end:
            item['page_str'] = page_start + '-' + page_end
        elif page_start:
            item['page_str'] = page_start + '-' + page_start
        else:
            item['page_str'] = page_end + '-' + page_end

        doi=response.xpath('//head/meta[@name="citation_doi"]/@content').extract_first()
        if doi:
            item['doi'] = doi
            sid = doi.split('/')[-1]
            item['sid'] = sid
        else:
            item['sid'] = '/'.join(response.url.split('/')[-3, -1])

        keywords = response.xpath('//head/meta[@name="citation_keywords"]/@content').extract()
        if keywords:
            item['keywords'] = ';'.join(keywords)

        src = 'ams'
        item['src'] = src

        date_str=response.xpath('//head/meta[@name="citation_online_date"]/@content').extract_first()
        if date_str:
            item['date_str']=date_str

        year=response.xpath('//head/meta[@name="citation_publication_date"]/@content').extract_first()
        item['year']=int(year)

        venue = {}
        venue['name'] = response.xpath('//head/meta[@name="citation_journal_title"]/@content').extract_first()
        venue['type'] = 1
        venue['sid'] = src
        item['venue'] = venue

        author_list = soup.find_all('meta', attrs={'name': 'citation_author'})
        if author_list:
            authors = []
            for index in range(len(author_list)):
                author = {
                    'name': ' '.join(author_list[index]['content'].replace('\n','').replace('\r','').replace('\t','').strip().split()),
                    'pos': index
                          }
                org = []
                email = []
                for tag in author_list[index].next_siblings:
                    if isinstance(tag, Tag):
                        if 'name' in tag.attrs and tag['name'] == 'citation_author_institution':
                            org.append(tag['content'].replace('\n','').replace('\r','').replace('\t','').strip(', '))
                        elif 'name' in tag.attrs and tag['name'] == 'citation_author_orcid':
                            author['orcid'] = tag['content']
                        elif 'name' in tag.attrs and tag['name'] == 'citation_author_email':
                            email.append(tag['content'])
                        else:
                            break
                if len(email) != 0:
                    author['email'] = ';'.join(email)
                if len(org) != 0:
                    author['org'] = org
                elif response.xpath('//div[@class="wd-jnl-art-author-affiliations"]/p[@class="mb-05"]/text()').extract():
                    affi = response.xpath('//div[@class="wd-jnl-art-author-affiliations"]/p[@class="mb-05"]/text()').extract()
                    author['org'] = affi.replace('\n','').replace('\r','').replace('\t','').strip(', ')
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

        if response.xpath('//div[@class="col-xs-12 col-md-10 col-md-offset-1"]/p[@align="left"][2]/a/@name').extract_first() == 'Abstract':
            item['abstract'] = response.xpath('//div[@class="col-xs-12 col-md-10 col-md-offset-1"]/p[@align="left"][2]').extract_first()

        reference_list = response.xpath('//div[@id="EnhancedReferences"]/span/ul/li').extract()
        if reference_list:
            item['reference'] = reference_list

        self.save_to_mongo(dict(item))

    def save_to_mongo(self, result):
        try:
            if self.db[self.MONGO_TABLE].insert(result):
                print('保存到MonGo成功')
        except Exception:
            print('存储到MonGo失败', result)