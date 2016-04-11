# written by Tom

import sys
import json
from scrapy.utils.project import get_project_settings

from scrapy.crawler import CrawlerProcess
from spiders.okcubot_spider import OKcuSpider
import os

import argparse
import pdb              #interactive python debugger


if __name__ == '__main__':

	# define parser to parse arguments from command line input.
	parser = argparse.ArgumentParser(prog='PROG', usage='python start.py username password [--o data_path] [--n max_num]')

	# define argument fields
	parser.add_argument('user', nargs='+', help='set a user and a password! (e.g. python start.py jack@gmail.com jackpassword)')	
	parser.add_argument('--o', nargs='?', help='the path for saving data', dest="path")
	parser.add_argument('--n', nargs='?', help='the max number of people who the program scrape data, this must be number.', dest="num", type=int)
	parser.add_argument('--u', nargs='?', help='a user name to be scraped', dest="target_user", type = lambda s : unicode(s, sys.getfilesystemencoding())) #input is unicode, but python 2 assumes ASCII
	parser.add_argument('--noskip', help='don\'t skip a user who was scraped before', dest="noskip", action="store_true")

	# parse arguments
	args = parser.parse_args()

	# parse username and password
	if len(args.user) == 2:
		user = args.user[0]
		password = args.user[1]
	else:
		parser.print_help()
		exit()

	# parse data path
	if args.path is not None:
		data_path = args.path
	else:
		absolute_path = os.path.dirname(os.path.abspath(__file__))
		path_seg = 	absolute_path.split('OKCubot2')
	
		if len(path_seg) == 2:
			data_path = path_seg[0] + "OKCubot2/" + "data/"
		else:
			data_path = "data/"

	# parse the max number of people to be scraped
	if args.num is not None:
		num = args.num
	else:
		num = 1 # the default max number
	# pdb.set_trace()
	target_user = args.target_user
	noskip = args.noskip

	crawler = CrawlerProcess(get_project_settings())
	crawler.crawl(OKcuSpider, user = user, password = password, path = data_path, max_num=num, target_user=target_user, noskip=noskip)

	crawler.start()
