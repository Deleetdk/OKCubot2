# written by Tom

import sys
import json
from scrapy.utils.project import get_project_settings

from scrapy.crawler import CrawlerProcess
from spiders.okcubot_spider import OKcuSpider
import os




if __name__ == '__main__':
	absolute_path = os.path.dirname(os.path.abspath(__file__))
	path_seg = 	absolute_path.split('OKCubot2')
	if len(path_seg) == 2:
		data_path = path_seg[0] + "OKCubot2/" + "data/"
	else:
		data_path = "data/"

	print data_path

	user = ""
	password = ""

	# get data from shell	
	if len(sys.argv) == 3:
		user = sys.argv[1]
		password = sys.argv[2]
	else:
		print "Please supply a user and a password! (e.g. python start.py jack@gmail.com jackpassword)"
		exit()

	crawler = CrawlerProcess(get_project_settings())
	crawler.crawl(OKcuSpider, user = user, password = password, path = data_path)

	crawler.start()
