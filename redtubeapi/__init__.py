#!/usr/bin/env
# -*- coding: utf-8 -*-
# author=Kristen
# date = 8/7/16R

import sys, os
import logging
from logging import FileHandler as _FileHandler
from collections import Counter, OrderedDict, MutableSet

_all_ = ['FileHandler']

from .utils import *
from .redtube import *

from geekfreak.redtube import (Collection, Database, REDTUBE, REDTUBE_CATEGORIES, RSS_FEED, RedClient, RedCollection,
                               RedTube, Row, SHD, __version__, _count, ascii, by_date, fetch, fetch_exact, fetch_like,
                               headings, video_id, get_username)

LOGFILE = os.path.join(os.path.dirname(__file__), 'redtube.log')
logging.basicConfig(filename=LOGFILE,
                    level=logging.DEBUG,
                    style='{',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format='{created};{levelname};{funcName};{msg}')



class FileHandler(_FileHandler):
	def __init__(self, filename= LOGFILE, *args, **kwargs):
		super(FileHandler, self).__init__(filename=filename,*args, **kwargs)
		self.setLevel(logging.DEBUG)
		self.__file__ = self.baseFilename
		name = os.path.basename(self.__file__).split('.')[0]
		self.set_name(name)
		#self.setFormatter('{created};{levelname};{funcName};{msg}')

	def __repr__(self):
		classname = self.__class__.__name__
		_name = self._name or os.path.basename(self.__file__).split('.')[0] or ''
		self.__file__ = self.baseFilename
		directory = os.path.dirname(self.__file__)
		base = os.path.basename(self.__file__)
		return '{}("{}")'.format(classname, _name)

	def read_file(self):
		records = []
		with open(self.__file__) as reader:
			lines = reader.read().splitlines()
			for line in lines:
				args = line.split(';')
				if len(args) == 4:
					args[0] = fmt_timestamp(args[0])
					records.append(args)
				else:
					records.append(args)
		return records



if __name__ == '__main__':
	LOGFILE = '/Applications/PycharmProjects1/proj/geekfreak/geekfreak/redtube/redtube.log'
	logging.basicConfig(level=logging.DEBUG,
	                    style='{',
	                    datefmt='%Y-%m-%d %H:%M:%S',
	                    format='{created};{levelname};{funcName};{msg}')
	Handler = FileHandler()
	logger = logging.getLogger('redtube')
	if len(logger.handlers) == 1:
		logger.removeHandler(logger.handlers[0])
		logger.addHandler(Handler)










