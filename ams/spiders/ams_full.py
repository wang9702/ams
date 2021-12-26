import scrapy
import pymongo
import re
import time, datetime
from bs4 import BeautifulSoup
from bs4 import Tag

class AmsSpider(scrapy.Spider):
    name = 'ams_full'
    allowed_domains = ['ams.org']
    # start_urls = ['https://www.ams.org/journals/era/1996-02-03/home.html']
    start_urls = ['https://www.ams.org/journals']
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
    }
    def __init__(self):
        self.mongo_client = pymongo.MongoClient(host="117.119.77.139",
                                                port=30019,
                                                username="gong",
                                                password="tipsikeg2012",
                                                authSource="ysu",
                                                authMechanism="SCRAM-SHA-1",
                                                )
        # self.mongo_client = pymongo.MongoClient('mongodb://localhost:27017')
        self.publisher_journal_db = self.mongo_client['ysu']
        self.collection = self.publisher_journal_db['ams_alldata']
        self.collection.ensure_index('url', unique=True)
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
        if len(article_list)==0:
            article_list = re.findall('<a href="(.*?)">Abstract, references and article information</a>', response.text)
        for article in article_list:
            url = response.urljoin(article)
            yield scrapy.Request(
                url=url,
                callback=self.parse_article,
                headers=self.header,
                meta=response.meta
            )
    def parse_article(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        item = {}
        item['ts'] = datetime.datetime.strptime(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                                                '%Y-%m-%d %H:%M:%S')

        url = [response.url]
        item['url'] = url

        lang = 'en'
        item['lang'] = lang

        title = ''
        if response.xpath('//head/meta[@name="citation_title"]/@content').extract_first():
            title = response.xpath('//meta[@name="citation_title"]/@content').extract_first()
            item['title'] = {lang: self.format(title)}
        elif ''.join(response.xpath('//p[@class="articleTitle"]/text()').extract()).strip():
            title = ''.join(response.xpath('//p[@class="articleTitle"]/text()').extract()).strip()
            item['title'] = {lang: self.format(title)}
        if title:
            raw_title = response.xpath('//div[@class="col-xs-12 col-md-10 col-md-offset-1"]/p[@class="articleTitle"][2]').extract_first()
            item['raw_title'] = {lang: raw_title}

            words = item['title'][lang].split()
            hash = ''
            for word in words:
                if word[0]:
                    hash = hash + word[0].lower()
            item['hash'] = hash

        issn = response.xpath('//meta[@name="citation_issn"]/@content').extract_first()
        item['issn'] = issn

        if len(response.xpath('//meta[@name="citation_issn"]/@content').extract())==2:
            eissn = response.xpath('//meta[@name="citation_issn"]/@content').extract()[1]
            item['eissn'] = eissn

        pdf_src = response.xpath('//meta[@name="citation_pdf_url"]/@content').extract()
        if pdf_src:
            item['pdf_src'] = pdf_src

        item['year'] = int(response.meta['year'])

        volume = response.xpath('//meta[@name="citation_volume"]/@content').extract_first()
        if volume:
            item['volume'] = volume
        else:
            item['volume'] = response.meta['year']

        issue = response.xpath('//meta[@name="citation_issue"]/@content').extract_first()
        if issue:
            item['issue'] = issue
        else:
            item['issue'] = '1'
        # item['year'] = 1996
        # item['volume'] = '2'
        # item['issue'] = '3'

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
            if len(doi.split('/')) == 2:
                sid = doi.split('/')[-1]
            else:
                sid = '/'.join(doi.split('/')[-2:])
            item['sid'] = sid
        else:
            item['sid'] = '/'.join(response.url.split('/')[-3:-1])

        src = 'ams'
        item['src'] = src

        keywords = response.xpath('//meta[@name="citation_keywords"]/@content').extract()
        if keywords:
            new_keywords = []
            for keyword in keywords:
                new_keywords.append(self.format(keyword).strip())
            if len(re.findall('<[\s\S]*?>', ';'.join(new_keywords).replace(',', ';'))) > 0:
                new_keywords_filter_list = re.sub('<[\s\S]*?>', '', ';'.join(new_keywords).replace(',', ';')).split(';')
                words = []
                for word in new_keywords_filter_list:
                    words.append(word.strip())
                new_keywords_filter = ';'.join(words)
            else:
                new_keywords_filter = ';'.join(new_keywords).replace(',', ';')
            item['keywords'] = {lang: new_keywords_filter}

        date_str = response.xpath('//meta[@name="citation_online_date"]/@content').extract_first()
        if date_str:
            item['date_str'] = date_str
        info={}
        span_list=response.xpath('//span[@class="bibDataTag"]')
        for span in span_list:
            if 'MathSciNet review:' in span.xpath('./text()').extract_first():
                mathscinet_review = span.xpath('./following-sibling::a//text()').extract_first()
                info['mathscinet_review'] = mathscinet_review.strip()
        text_list = response.xpath('//p[@align="left"]//text()').extract()
        for text in text_list:
            if 'Primary' in text:
                info['msc'] = text.strip()
        if info:
            item['info'] = info

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
        foot_author_list = response.xpath('//div[@class="col-xs-12 col-md-10 col-md-offset-1"]/p[last()]/strong/text()').extract()
        if author_list:
            authors = []
            for index in range(len(author_list)):
                author = {'pos': index,
                          'name': {lang: self.format(author_list[index]['content']).replace(',', '')}}
                if foot_author_list and len(author_list) == len(foot_author_list):
                    author['name'] = {lang: foot_author_list[index]}
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
                                email_list.append(self.format(email.replace(',', ';').replace(' and ', ';').replace(' or ', ';').replace('E-mail address:', ';')))
                            else:
                                email_list.append(self.format(tag['content'].replace(',', ';').replace(' and ', ';').replace(' or ', ';').replace('E-mail address:', ';')))
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

        if re.findall(r'(<a href="/jourhtml/abstracthelp.html#addinfo">Additional Notes:</a>[\s\S]*?)<br />',response.text, re.S | re.M):
            item['notes'] = [{lang: re.findall(r'(<a href="/jourhtml/abstracthelp.html#addinfo">Additional Notes:</a>[\s\S]*?)<a href="/jourhtml/abstracthelp.html#addinfo">Article copyright:</a>',response.text, re.S | re.M)[0]}]

        if response.xpath('//div[@class="col-xs-12 col-md-10 col-md-offset-1"]/p[@align="left"][2]/a/@name').extract_first() == 'Abstract':
            if re.findall(r'(<a name="Abstract" id="Abstract" class="abstractSubHead">Abstract: </a>[\s\S]*?)<form action="">',response.text, re.S | re.M):
                item['abstract'] = {lang: re.findall(r'(<a name="Abstract" id="Abstract" class="abstractSubHead">Abstract: </a>[\s\S]*?)<form action="">',response.text, re.S | re.M)[0]}
            elif re.findall(r'(<a name="Abstract" id="Abstract" class="abstractSubHead">Abstract: </a>[\s\S]*?)</p>',response.text, re.S | re.M):
                item['abstract'] = {lang: re.findall(r'(<a name="Abstract" id="Abstract" class="abstractSubHead">Abstract: </a>[\s\S]*?)</p>',response.text, re.S | re.M)[0]}

        reference_list = response.xpath('//div[@id="EnhancedReferences"]/span/ul/li').extract()
        if reference_list:
            references = []
            for reference in reference_list:
                references.append({lang: reference})
            item['reference'] = references

        correction_list = re.findall('(<span class="bibDataTag">Original Article:</span>[\s\S]*?<a.*?>[\s\S]*?</a>)', response.text, re.S|re.M)
        if correction_list:
            corrections = []
            for correction in correction_list:
                corrections.append({lang: correction})
            item['corrections'] = corrections

        self.save_to_mongo(dict(item))

    def format(self, s):
        return ' '.join(s.replace('\n', '').replace('\r', '').replace('\t', '').strip(',; ').split())

    def update_ref(self, id):
        try:
            if self.publisher_journals_urls_col.update_one({'_id': id}, {'$set': {'ref': 1}}):
                print('更新ref成功')
        except Exception as e:
            print(e)
            print("更新ref失败")

    def save_to_mongo(self, item):
        try:
            if self.collection.insert(item):
                # self.update_ref(id)
                print('保存到MonGo成功')
        except Exception as e:
            print(e)


#https://www.ams.org/journals/proc/1970-026-01/S0002-9939-1970-0265832-2/
#https://www.ams.org/journals/proc/1970-026-04/S0002-9939-1970-0267534-5/


# re.search('<a href="/jourhtml/abstracthelp.html#addinfo">Keywords:</a>\n<img.*?alt="(.*?)">([\s\S]*?)<img.*?>')
# re.findall(r'<IMG.*?BORDER="0".*?ALT="(.*?)">([\s\S]*?)', response.text)
# IMG WIDTH="22" HEIGHT="20" ALIGN="BOTTOM" BORDER="0" SRC="images/img1.gif" ALT="$ E$"