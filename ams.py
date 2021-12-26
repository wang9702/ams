# from scrapy.cmdline import execute
# import sys
# import os
# import time,datetime
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 你需要将此处的spider_name替换为你自己的爬虫名称
# __file__表示当前.py文件的路径
# os.path.dirname(__file__)表示当前.py文件所在文件夹的路径
# os.path.abspath()表示当前.py文件的绝对路径
# while True:
#     now = datetime.datetime.now()
#     print(now.strftime("%Y-%m-%d %X"))
#     execute(['scrapy', 'crawl', 'baike'])
#     time.sleep(86400)

from scrapy import cmdline
cmdline.execute("scrapy crawl new_proc".split())