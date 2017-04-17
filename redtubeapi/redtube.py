#!/usr/bin/env
# -*- coding: utf-8 -*-
# author=Kristen
# date = 8/7/16

import sys, os
from urllib.request import urlopen, Request #todo requests
from urllib.error import URLError
from sys import version_info, argv
from optparse import OptionParser
from datetime import datetime, timedelta
from urllib.parse import urlencode
from base64 import b64decode
from weakref import ref
from json import loads
from copy import copy
from math import pow
from requests import get
import sqlite3 as sqlite
import os
from operator import itemgetter
from collections import Counter, OrderedDict

from grab import Grab
import re
import isodate
import feedparser
from operator import itemgetter, attrgetter
import logging
from collections import Counter, OrderedDict, MutableSet
from functools import partial
from urllib.parse import urlencode

from .utils import *

#todo database functionality for many types #couchdb #redis #sqlite #mysql (?) #librabbit ?

__all__ = ['Collection', 'Database', 'REDTUBE', 'REDTUBE_CATEGORIES', 'RSS_FEED', 'RedClient', 'RedCollection',
           'RedTube', 'Row', 'SHD', '__version__', '_count', 'ascii', 'by_date', 'fetch', 'fetch_exact', 'fetch_like',
           'headings', 'video_id', 'get_username']

REDTUBE_CATEGORIES = ('id', 'url', 'title', 'uploader', 'views', 'rating', 'votes', 'comments', 'upload_date','duration', 'tags', 'categories', 'stars')
SHD = '/Users/Kristen/Sexual_Health_Data' #todo
REDTUBE = '/Users/Kristen/Sexual_Health_Data/redtube.db' #remove
headings = ('id', 'url', 'title', 'published', 'duration', 'timing', 'tags', 'stars')
RSS_FEED = "http://feeds.feedburner.com/redtube/videos"

__version__ = '0.5.0'
ascii = ' 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
#create_file
#create_commit
#commit

def get_datetime(s):
	try:
		return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
	except AttributeError:
		return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

def get_username(url=None):
	G = Grab()
	u = G.request(url=url)
	name = u.select('//td/a[@rel="nofollow"]/@href').node().strip('/')
	text = u.select('//td/a[@rel="nofollow"]/text()').node().lower()
	if name == text:
		return name
	elif name and not text:
		return name
	elif text and not name:
		return text
	return None


class Database(object):
	"""cursor.execute('''UPDATE redtube SET upload_date = ? WHERE id = ? ''',
               (str(first.upload_date), first.id))"""
	DROP_TABLE = "DROP TABLE IF EXISTS {}"
	directory = SHD

	def __init__(self, db_file='/Users/Kristen/Sexual_Health_Data/redtube.db',
	             source_file=None, directory=SHD, *args, **kwargs):
		if kwargs:
			self.name = kwargs.get('name', 'redtube.db') or self.__name__ or 'database.db'
		else:
			self.name = self.__name__ or 'database.db'
		self.db_file = db_file or self.__file__ or os.path.dirname(os.path.realpath(directory)) + self.name
		self.source_file = source_file or None
		self.connection = None
		self.names = None
		#self.connect()

	def connect(self):
		import sqlite3
		self.connection = sqlite3.connect(self.db_file) or sqlite3.connect(":memory:")
		self.cursor = self.connection.cursor()

	def get_table_names(self):
		if self.connection:
			sql = self.connection.execute('SELECT name FROM sqlite_master').fetchall()
			self.names = tuple([row[0] for row in sql])
		else:
			self.names = 'None found'
		return self.names

	def get_table_sql(self, table_names=None):
		results = []
		table_names = table_names or self.names

		if isinstance(table_names, (tuple, list, set)):
			for table in table_names:
				sql_statement = 'SELECT sql FROM sqlite_master WHERE type = "table" AND name = "{}"'.format(table)
				name_statement = 'SELECT * from {}'.format(table)
				cnt = self.connection.execute('SELECT COUNT(*) FROM {}'.format(table)).fetchone()[0]
				sql = self.connection.execute(sql_statement).fetchone()[0]
				cols = tuple(zip(*self.connection.execute(name_statement).description))[0]
				name = table
				result = dict(name=name, sql=sql, columns=cols, count=cnt)
				results.append(result)

		elif isinstance(table_names, str):
			sql_statement = 'SELECT sql FROM sqlite_master WHERE type = "table" AND name = "{}"'.format(table_names)
			name_statement = 'SELECT * from {}'.format(table_names)
			cnt = self.connection.execute('SELECT COUNT(*) FROM {}'.format(table)).fetchone()[0]
			cols = tuple(zip(*self.connection.execute(name_statement).description))[0]
			sql = self.connection.execute(sql_statement).fetchone()[0]
			name = table_names
			result = dict(name=name, sql=sql, columns=cols, count=cnt)
			results = result
		else:
			results = None

		self.tables= results
		return self.tables

	def get_table_count(self, table_names=None):
		results = []
		table_names = table_names or self.names
		if isinstance(table_names, (tuple, list, set)):
			for table in table_names:
				cnt = self.connection.execute('SELECT COUNT(*) FROM {}'.format(table)).fetchone()[0]
				name = table
				result = dict(name=name, count=cnt)
				results.append((name, cnt))

		elif isinstance(table_names, str):
			cnt = self.connection.execute('SELECT COUNT(*) FROM {}'.format(table_names)).fetchone()[0]
			name = table_names
			result = dict(name=name, count=cnt)
			results.append((name, cnt))

		else:
			results = None
		return results

	def __repr__(self):
		return '<{} ({})>'.format(self.__class__.__name__, self.db_file)

	def get_rows(self, table='', params = '*', i=-1):
		cursor = self.connection.execute('SELECT {params} from {table}'.format(params=params, table=table))
		names = list(zip(*cursor.description))[0]
		if i != -1:
			results = cursor.fetchmany(i)
		else:
			results = cursor.fetchall()
		return results

	def fetch_exact(self, table=None, column=None, parameter=None):
		statement = 'SELECT * FROM {table} WHERE {column} = "{parameter}"'.format(table=table, column=column,
		                                                                          parameter=parameter)
		print(statement)
		results = self.execute(statement).fetchall()
		if len(results) == 1:
			return results[0]
		elif len(results) == 0:
			return column, parameter
		else:
			return results

	def fetch_like(self, table=None, column=None, parameter=None):
		statement = 'SELECT * FROM {table} WHERE {column} LIKE "%{parameter}%" ORDER BY {column} ASC'.format(
			table=table, column=column,
			parameter=parameter)
		print(statement)
		results =self.execute(statement).fetchall()
		if len(results) == 1:
			return results[0]
		elif len(results) == 0:
			return column, parameter
		else:
			return results

class RedTube(Database):
	__file__ = '/Users/Kristen/Sexual_Health_Data/redtube.db'
	__name__ = 'redtube'

	def activate(self):
		self.connect()
		self.get_table_names()
		self.get_table_sql()

	def by_id(self, arg=None):
		if isinstance(arg, Row):
			parameter = arg.id
		elif isinstance(arg, (list, tuple)):
			parameter = arg[0]
		elif isinstance(arg, str):
			parameter = arg

		statement = 'SELECT * FROM {table} WHERE {column} = "{parameter}"'.format(table='redtube', column='id',
		                                                          parameter=parameter)
		results = self.connection.execute(statement).fetchone()
		return results

	def insert(self, Row=None):
		exists = bool(self.by_id(Row))
		print('Already present:', exists)
		cur = self.cursor or self.connection.cursor()
		row = Row.as_table()
		conn = self.connection
		try:
			cur.execute("INSERT INTO redtube VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);", row)
			print('row added', row)
		except sqlite.IntegrityError:
			print('Duplicate:', row)
			pass
		conn.commit()

	def safe_insert(self, Row=None):
		exists = bool(self.by_id(Row))
		if exists:
			print(RecordExistsError(Row.id, 'redtube'))
		elif not exists:
			self.insert(Row=Row)
		else:
			pass

class RedClient(object):
	''' Python RedTube API Client '''
	server =BASE_URL= 'http://api.redtube.com/'
	thumbnail_sizes = ['all', 'small', 'medium', 'medium1', 'medium2', 'big']
	ORDERING = ['mostviewed', 'newest', 'rating']
	PERIOD = ['alltime', 'monthly', 'weekly']
	IS_ACTIVE = 'http://api.redtube.com/?data=redtube.Videos.isVideoActive&video_id='

	def __init__(self):
		self._categories = []
		self._tags = []
		self._stars = []
		self._detailed_stars = []

	def _request(self, data, **kwargs):
		from urllib.parse import urlencode
		from urllib.request import urlopen, Request
		from json import loads
		kwargs.update({'output': 'json', 'data': data})
		#print('KWARGS:', kwargs)
		#print('DATA:', data)
		url = '%s?%s' % (self.server, urlencode(kwargs))
		print('URL:', url)
		request = Request(str(url),
		                  headers={
			'User-Agent': 'RedTubeClient/%s (Python/%s)' % (
				__version__, '.'.join(map(str, version_info[:-2])))
		                  })

		try:
			response = urlopen(request).read().decode('utf-8')
			result = loads(response)
			result.update(kwargs)
			result['url'] = url
			return result
		except URLError as e:
			raise RedException(str(e))

	def search(
			self, query=None, category=None, tags=[],
			stars=[], thumbnail_size=None, page=1, ordering='newest', period='alltime'):
		"""param: ordering (optional). Possible values are `newest`, `mostviewed`, `rating`.
			param: period (optional but ONLY if 'ordering' is set. Possible values are weekly, monthly, alltime.

		Args:
			query:
			category:
			tags:
			stars:
			thumbnail_size:
			page: 1 (default)

		Returns:

		"""

		if thumbnail_size not in self.thumbnail_sizes:
			thumbnail_size = None
		if page < 1:
			page = 1

		data = {'page': int(page), 'data': 'redtube.Videos.searchVideos'}
		if query:
			data['search'] = query
		if category:
			data['category'] = category
		if tags:
			data['tags[]'] = tags
		if stars:
			data['stars[]'] = stars
		if thumbnail_size:
			data['thumbsize'] = thumbnail_size
		if ordering:
			data['ordering'] = ordering
		if period and ordering:
			data['period'] = period


		d = {k:v for k,v in data.items() if v is not None}
		print(d)


		return Collection(**d)
		#return RedCollection(self, data)

	def by_id(self, id, thumbnail_size=None):
		if thumbnail_size not in self.thumbnail_sizes:
			thumbnail_size = None

		data = {'video_id': id}
		if thumbnail_size:
			data['thumbsize'] = thumbnail_size

		try:
			result = self._request('redtube.Videos.getVideoById', **data)['video']
			return Row(**result)
		except KeyError:
			return None

	def __getitem__(self, id):
		return self.by_id(id)

	@property
	def categories(self):
		if not self._categories:
			self._categories = [
				entry['category'] for entry in
				self._request(
					'redtube.Categories.getCategoriesList'
				)['categories']
				]
		return self._categories

	@property
	def tags(self):
		if not self._tags:
			self._tags = [
				entry['tag']['tag_name'] for entry in
				self._request('redtube.Tags.getTagList')['tags']
				]
		return self._tags

	@property
	def stars(self):
		if not self._stars:
			self._stars = [
				entry['star']['star_name'] for entry in
				self._request('redtube.Stars.getStarList')['stars']
				]
		return self._stars

	@property
	def detailed_stars(self):
		if not self._detailed_stars:
			self._detailed_stars = [
				(entry['star']['star_name'], entry['star']['star_thumb'], entry['star']['star_url']) for entry in
				self._request('redtube.Stars.getStarDetailedList')['stars']
				]
		return self._detailed_stars

class Row(object):
	ALLOWED = set(' 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz')

	def __init__(self, **kwargs):
		self.kwargs = kwargs
		self.votes = int(kwargs.get('ratings')) if hasattr(self, 'ratings') else kwargs.get('votes')
		self.title = ''.join([char for char in kwargs.get('title').lower() if char in self.ALLOWED])
		self.url = kwargs.get('url', '')
		self.views = kwargs.get('views', '0')

		for k,v in kwargs.items():
			try:
				self.__setattr__(k,v)
			except AttributeError:
				pass

	@property
	def upload_date(self):
		fake_date = '1900-01-01 00:00:01'
		if 'upload_date' in self.kwargs:
			upload_date = self.kwargs.get('upload_date', get_datetime(fake_date))
		elif 'publish_date' in self.kwargs:
			upload_date = get_datetime(self.kwargs.get('publish_date', fake_date))
		else:
			upload_date = get_datetime(self.kwargs.get('publish_date', fake_date))
		return upload_date

	@property
	def duration(self):
		timing = self.kwargs.get('duration')
		if isinstance(timing, int):
			duration = timing
		else:
			duration = int(sum([x[1] * pow(60, x[0]) for x in enumerate(map(int, timing.split(':')[::-1]))]))
		return duration

	@property
	def _tags(self):
		raw_tags = self.kwargs.get('tags')
		if isinstance(raw_tags, (list, tuple)):
			tags = [tag['tag_name'].lower() for tag in raw_tags]
		elif isinstance(raw_tags, dict):
			tags = {v.lower(): k for k, v in kwargs.get('tags', dict()).items()}
		return tags

	@property
	def id(self):
		return self.kwargs.get('video_id') or self.kwargs.get('id')

	def get_username(self):
		G = Grab()
		u = G.request(url=self.url)
		name = u.select('//td/a[@rel="nofollow"]/@href').node().strip('/')
		text = u.select('//td/a[@rel="nofollow"]/text()').node().lower()
		if name == text:
			self.uploader = name
			return self.uploader
		elif name and not text:
			self.uploader = name
			return self.uploader
		elif text and not name:
			self.uploader = text
			return self.uploader
		return None

	def get_info(self):
		from grab import Grab
		G = Grab()
		u = G.request(url=self.url)
		re_likes = u.select('//span[@class="percent-likes"]').text('0%')
		re_comments = u.select('//a[@class="comments-btn"]').text('comments (0)')
		try:
			likes = re.search('(\d\d\d|\d\d|\d)%', re_likes).group(1)
		except AttributeError:
			likes =''
		try:
			comments = re.search('\s?comments\s?\((\d\d\d\d|\d\d\d|\d\d|\d)\)', re_comments).group(1)
		except AttributeError:
			comments = ''
		tags = '/'.join(u.select('//td[preceding-sibling::td="TAGS"]/a').text_list([])).lower()
		categories = '/'.join(u.select('//td[preceding-sibling::td="FROM"][3]/*').text_list([])).lower()
		stars = '/'.join(u.select('//td[preceding-sibling::td="PORNSTARS"]').text_list([])).lower()
		username = u.select('//td/a[@rel="nofollow"]/@href').node().strip('/') or u.select(
			'//td/a[@rel="nofollow"]/text()').node().lower() or ''
		self.info= dict(likes=likes,
					tags=tags,
					categories=categories,
					comments=comments,
					uploader=username,
					stars=stars)
		if username:
			self.uploader = username
		for k, v in self.info.items():
			self.__setattr__(k, v)

	def expanded(self):
		try:
			return bool(self.info)
		except AttributeError:
			return False

	def get_tags(self):
		tags = self.tags
		return tags.split('/')

	@property
	def active(self):
		url = 'http://api.redtube.com/?data=redtube.Videos.isVideoActive&video_id={}'.format(self.id)
		self._active = bool(get(url).json().get('active', {}).get('is_active'))
		return self._active

	def __repr__(self):
		return '<%s[%s] "%s">' % (self.__class__.__name__, self.id, self.title)

	def as_table(self):
		attrs = attrgetter('id', 'url', 'title', 'uploader', 'views', 'rating', 'votes', 'comments', 'upload_date',
						   'duration', 'tags', 'categories', 'stars')
		try:
			return attrs(self)
		except AttributeError:
			self.get_info()
			return attrs(self)

	def _asdict(self):
		keys = ('id', 'url', 'title', 'uploader', 'views', 'rating', 'votes', 'comments', 'upload_date',
		        'duration', 'tags', 'categories', 'stars')
		vals = self.as_table()
		return dict(zip(keys, vals))

class Collection(list):
	def __init__(self, **kwargs):
		kwargs.update({'output': 'json'})
		url = '%s?%s' % ('http://api.redtube.com/', urlencode(kwargs))
		# req = request(data='redtube.Videos.getDeletedVideos', ordering='newest', period='all', page=page)
		json = get(url).json()

		super(Collection, self).__init__(Row(**entry.get('video')) for entry in json.get('videos'))
		self.total = int(json.get('count'))
		self._client = None
		self.query = kwargs.get('search','')
		self.kwargs = kwargs
		self.page = kwargs.get('page', 1)

	@property
	def client(self):
		if self._client() is None:
			return RedClient()
		return self._client()

	def __next__(self):
		if len(self) > self.total:
			return None
		kwargs = copy(self.kwargs)
		page = self.page + 1
		self.page += 1
		kwargs.update({'page': page})
		#data = kwargs.pop('data')
		self.extend(self.__class__(**kwargs))
		return self.__class__(**kwargs)

	def populate(self, database=None):
		while self.total > len(self):
			[database.insert(row) for row in next(self)]

	def get_page(self, page=1):
		kwargs = copy(self.kwargs)
		page = page
		kwargs.update({'page': page})
		data = kwargs.pop('data')
		return self.__class__(data, **kwargs)

RedCollection = Collection
# Client._request('redtube.Videos.getVideoById', **{'video_id': '1675468'})
# Client._request('redtube.Videos.searchVideos', **data)

#r.execute('SELECT id,url,upload_date FROM redtube').fetchone()

def _count(self, table='redtube', column=None, parameter=None):
	statement = 'SELECT COUNT(*) FROM {table} WHERE {column} LIKE "%{parameter}%"'.format(table=table, column=column,
																						  parameter=parameter)
	print(statement)
	self = self.connection
	return self.execute(statement).fetchone()[0]

def fetch_like(self, table, column, parameter):
	statement = 'SELECT * FROM {table} WHERE {column} LIKE "%{parameter}%" ORDER BY {column} ASC'.format(table=table, column=column,
																				   parameter=parameter)
	print(statement)
	self = self.connection
	return self.execute(statement).fetchall()

def fetch(self, table, column, parameter):
	statement = 'SELECT * FROM {table} WHERE {column} = "{parameter}" ORDER BY {column} ASC'.format(table=table, column=column,
																			  parameter=parameter)
	print(statement)
	self = self.connection
	results = self.execute(statement).fetchall()
	if len(results) == 1:
		return results[0]
	return results

def fetch_exact(self, table, column, parameter):
	statement = 'SELECT * FROM {table} WHERE {column} = "{parameter}"'.format(table=table, column=column,
																				   parameter=parameter)
	print(statement)
	self = self.connection
	results = self.execute(statement).fetchall()
	if len(results) == 1:
		return results[0]
	elif len(results) == 0:
		return parameter
	else:
		return results

def video_id(id=None):
	video = partial(fetch, self=None, table='redtube', column='id')
	return video(parameter=id)

def by_date(date=None):
	upload_date = partial(fetch_like, self=None, table='redtube', column='upload_date')
	return upload_date(parameter=date)

#p = partial(fetch_exact, self=r, table='redtube', column='url')
#date = partial(fetch_like, self=r, table='redtube', column='upload_date')


# data = {k:v for k,v in data.items() if v is not None}


def main(args=argv[1:]):
	parser = OptionParser(version=__version__, description=RedClient.__doc__)
	parser.add_option(
		'-q', '--query', dest="query", help='Query string', metavar='QRY'
	)
	parser.add_option(
		'-t', '--tag', dest='tags', action="append", default=[],
		help='Tags to search', metavar='TAG'
	)

	options = parser.parse_args(args)[0]

	for video in RedClient().search(query=options.query, tags=options.tags):
		print('[%s] %s: %s' % (video.timing, video.title, video.url))

if __name__ == '__main__':
	main(argv[1:])

