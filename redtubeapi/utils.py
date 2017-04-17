#!/usr/bin/env
# -*- coding: utf-8 -*-
# author=Kristen
# date = 8/8/16

import sys, os

from collections import Counter, OrderedDict, MutableSet,namedtuple
import logging
import datetime
import re
from logging import FileHandler as _FileHandler
from requests import get
import feedparser
from kgerringrc import *
from operator import attrgetter, itemgetter

__all__ = ['Cache', 'Entry', 'FeedParser', 'OrderedSet', 'RecordExistsError', 'RedException', 'RSS_FEED', '_parse_date_w3dtf', 'all_deleted',
 'change_page', 'getCategoriesList', 'getDeletedVideos', 'getRedtubeVideos', 'getStarDetailedList', 'getStarList',
 'getTagList', 'get_datetime', 'get_deleted_videos', 'get_entry', 'get_feed', 'get_info', 'odict', 'parse_datestr',
 'prune_dict', 'redtuple', 'request', 'row_is_expanded', 'searchVideos', 'username']

startTime = datetime.datetime.fromtimestamp(logging._startTime)
RSS_FEED = RSSFEED = "http://feeds.feedburner.com/redtube/videos"


def get_feed(url=RSS_FEED):
	feed = feedparser.parse(RSS_FEED).get('entries')
	return feed

def prune_dict(d):
	data = {k: v for k, v in d.items() if v is not None}

#def _parse_date_w3dtf(datestr, timetuple=True):
#	timezonenames = {
#		'ut' : 0, 'gmt': 0, 'z': 0,
#		'adt': -3, 'ast': -4, 'at': -4,
#		'edt': -4, 'est': -5, 'et': -5,
#		'cdt': -5, 'cst': -6, 'ct': -6,
#		'mdt': -6, 'mst': -7, 'mt': -7,
#		'pdt': -7, 'pst': -8, 'pt': -8,
#		'a'  : -1, 'n': 1,
#		'm'  : -12, 'y': 12,
#	}
#	if not datestr.strip():
#		return None
#	parts = datestr.lower().split('t')
#	if len(parts) == 1:
#		# This may be a date only, or may be an MSSQL-style date
#		parts = parts[0].split()
#		if len(parts) == 1:
#			# Treat this as a date only
#			parts.append('00:00:00z')
#	elif len(parts) > 2:
#		return None
#	date = parts[0].split('-', 2)
#	if not date or len(date[0]) != 4:
#		return None
#	# Ensure that `date` has 3 elements. Using '1' sets the default
#	# month to January and the default day to the 1st of the month.
#	date.extend(['1'] * (3 - len(date)))
#	try:
#		year, month, day = [int(i) for i in date]
#	except ValueError:
#		# `date` may have more than 3 elements or may contain
#		# non-integer strings.
#		return None
#	if parts[1].endswith('z'):
#		parts[1] = parts[1][:-1]
#		parts.append('z')
#	# Append the numeric timezone offset, if any, to parts.
#	# If this is an MSSQL-style date then parts[2] already contains
#	# the timezone information, so `append()` will not affect it.
#	# Add 1 to each value so that if `find()` returns -1 it will be
#	# treated as False.
#	loc = parts[1].find('-') + 1 or parts[1].find('+') + 1 or len(parts[1]) + 1
#	loc = loc - 1
#	parts.append(parts[1][loc:])
#	parts[1] = parts[1][:loc]
#	time = parts[1].split(':', 2)
#	# Ensure that time has 3 elements. Using '0' means that the
#	# minutes and seconds, if missing, will default to 0.
#	time.extend(['0'] * (3 - len(time)))
#	tzhour = 0
#	tzmin = 0
#	if parts[2][:1] in ('-', '+'):
#		try:
#			tzhour = int(parts[2][1:3])
#			tzmin = int(parts[2][4:])
#		except ValueError:
#			return None
#		if parts[2].startswith('-'):
#			tzhour = tzhour * -1
#			tzmin = tzmin * -1
#	else:
#		tzhour = timezonenames.get(parts[2], 0)
#	try:
#		hour, minute, second = [int(float(i)) for i in time]
#	except ValueError:
#		return None
#	# Create the datetime object and timezone delta objects
#	try:
#		stamp = datetime(year, month, day, hour, minute, second)
#	except ValueError:
#		return None
#	delta = timedelta(0, 0, 0, 0, tzmin, tzhour)
#	# Return the date and timestamp in a UTC 9-tuple
#	if timetuple:
#		try:
#			return (stamp - delta).utctimetuple()
#		except (OverflowError, ValueError):
#			return None
#	if not timetuple:
#		try:
#			return (stamp - delta)
#		except (OverflowError, ValueError):
#			return None
#
#def parse_datestr(datestr):
#	return _parse_date_w3dtf(datestr, timetuple=False)
#

class odict(OrderedDict):
	def __init__(self, *args, **kwargs):
		super(odict, self).__init__(*args, **kwargs)
	def __getattr__(self, attr):
		return self[attr]
	def __repr__(self):
		results = []
		for k, v in self.items():
			item = '"{}": {}'.format(k, v)
			results.append(item)
		display = ', '.join(results)
		final = '{' + display + '}'
		return final

class OrderedSet(MutableSet):
	SLICE_ALL = slice(None)
	"""
	An OrderedSet is a custom MutableSet that remembers its order, so that
	every entry has an index that can be looked up.
	"""
	import collections
	def __init__(self, iterable=None):
		self.items = []
		self.map = {}
		if iterable is not None:
			self |= iterable

	def __len__(self):
		return len(self.items)

	def __getitem__(self, index):
		"""
		Get the item at a given index.
		If `index` is a slice, you will get back that slice of items. If it's
		the slice [:], exactly the same object is returned. (If you want an
		independent copy of an OrderedSet, use `OrderedSet.copy()`.)
		If `index` is an iterable, you'll get the OrderedSet of items
		corresponding to those indices. This is similar to NumPy's
		"fancy indexing".
		"""
		if index == self.SLICE_ALL:
			return self
		elif hasattr(index, '__index__') or isinstance(index, slice):
			result = self.items[index]
			if isinstance(result, list):
				return OrderedSet(result)
			else:
				return result
		elif self.is_iterable(index):
			return OrderedSet([self.items[i] for i in index])
		else:
			raise TypeError("Don't know how to index an OrderedSet by %r" % index)

	def is_iterable(obj):
		"""
		Are we being asked to look up a list of things, instead of a single thing?
		We check for the `__iter__` attribute so that this can cover types that
		don't have to be known by this module, such as NumPy arrays.
		Strings, however, should be considered as atomic values to look up, not
		iterables. The same goes for tuples, since they are immutable and therefore
		valid entries.
		We don't need to check for the Python 2 `unicode` type, because it doesn't
		have an `__iter__` attribute anyway.
		"""
		return hasattr(obj, '__iter__') and not isinstance(obj, str) and not isinstance(obj, tuple)
	def copy(self):
		return OrderedSet(self)

	def __getstate__(self):
		if len(self) == 0:
			# The state can't be an empty list.
			# We need to return a truthy value, or else __setstate__ won't be run.
			#
			# This could have been done more gracefully by always putting the state
			# in a tuple, but this way is backwards- and forwards- compatible with
			# previous versions of OrderedSet.
			return (None,)
		else:
			return list(self)

	def __setstate__(self, state):
		if state == (None,):
			self.__init__([])
		else:
			self.__init__(state)

	def __contains__(self, key):
		return key in self.map

	def find_similar(self, term, max_dist=0.8):
		from jellyfish import jaro_distance
		if exact:
			found = [item for item in self.items if item.__contains__(term)]
		else:
			found = [(item, self.index(item)) for item in self.items if
			         term or term.lower() or term.upper() or term.title() in item]
		if found is None:
			return None
		else:
			return dict(found)

	def add(self, key):
		"""
		Add `key` as an item to this OrderedSet, then return its index.
		If `key` is already in the OrderedSet, return the index it already
		had.
		"""
		if key not in self.map:
			self.map[key] = len(self.items)
			self.items.append(key)
		return self.map[key]
	append = add

	def update(self, sequence):
		"""
		Update the set with the given iterable sequence, then return the index
		of the last element inserted.
		"""
		item_index = None
		try:
			for item in sequence:
				item_index = self.add(item)
		except TypeError:
			raise ValueError('Argument needs to be an iterable, got %s' % type(sequence))
		return item_index

	def index(self, key):
		"""
		Get the index of a given entry, raising an IndexError if it's not
		present.
		`key` can be an iterable of entries that is not a string, in which case
		this returns a list of indices.
		"""
		if self.is_iterable(key):
			return [self.index(subkey) for subkey in key]
		return self.map[key]

	def pop(self):
		"""
		Remove and return the last element from the set.

		Raises KeyError if the set is empty.
		"""
		if not self.items:
			raise KeyError('Set is empty')

		elem = self.items[-1]
		del self.items[-1]
		del self.map[elem]
		return elem

	def discard(self, key):
		"""
		Remove an element.  Do not raise an exception if absent.
		The MutableSet mixin uses this to implement the .remove() method, which
		*does* raise an error when asked to remove a non-existent item.
		"""
		if key in self:
			i = self.items.index(key)
			del self.items[i]
			del self.map[key]
			for k, v in self.map.items():
				if v >= i:
					self.map[k] = v - 1

	def clear(self):
		"""
		Remove all items from this OrderedSet.
		"""
		del self.items[:]
		self.map.clear()

	def __iter__(self):
		return iter(self.items)

	def __reversed__(self):
		return reversed(self.items)

	def __eq__(self, other):
		if isinstance(other, OrderedSet):
			return len(self) == len(other) and self.items == other.items
		try:
			other_as_set = set(other)
		except TypeError:
			# If `other` can't be converted into a set, it's not equal.
			return False
		else:
			return set(self) == other_as_set

	@property
	def _as_dict(self):
		"""The reverse of map"""
		return dict(enumerate(self.items))

	def __repr__(self):
		if len(self.items) < 10:
			return 'OrderedSet(%r)' % self.items
		else:
			return 'OrderedSet of %d items like %r' % (len(self.items), self[0])

class Cache(object):
	def __init__(self, *args, **kwargs):
		self.words = OrderedSet(kwargs.get('replace').words.items) if 'replace' in kwargs else OrderedSet()
		self.results = odict(kwargs.get('replace').results.items()) if 'replace' in kwargs else odict()

	def __setitem__(self, k, v):
		if k not in self.words:
			self.words.add(k)
			self.results[k] = v

	def __getitem__(self, word):
		if word in self.words.items:
			answer = self.results.get(word)
			if answer is None:
				self.words.remove(word)
				return None
			return answer
		elif word in self.results.keys() and self.results.get(word) is not None and word not in self.words.items:
			self.words.add(word)
			self.results.move_to_end(word)
			return {word: self.results.get(word)}

		elif word not in self.words.items and word not in self.results.keys():
			return None
		return None

class RedException(Exception): pass

class RecordExistsError(Exception):
	def __init__(self, record='', table=''):
		self.record = record
		self.table = table
		self.msg = 'Record "{}" already exists for table "{}"'.format(self.record, self.table)

	def __str__(self):
		return repr(self.msg)

def get_datetime(s):
	try:
		return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
	except AttributeError:
		return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

def searchVideos(self, **data):
	client = self or RedClient()
	query_type = 'redtube.Videos.searchVideos'
	data = data
	result = client._request(query_type, **data)
	result.update(data)
	return result

def change_page(url=None, page=None):
	url = url or 'http://api.redtube.com/?data=redtube.Videos.searchVideos&page=1'
	pattern = 'page=(\d+)'
	current_page = re.search(pattern, url).group(1)
	pagestr = 'page={}'.format(int(current_page) + 1) or 'page={}'.format(page)
	newstr = re.sub(pattern, pagestr, url)
	return newstr

redtuple = namedtuple('RedTube', (
'id', 'url', 'title', 'uploader', 'views', 'rating', 'votes', 'comments', 'upload_date', 'duration', 'tags',
'categories', 'stars'))

def getCategoriesList():
	raw_categories = get('http://api.redtube.com/?data=redtube.Categories.getCategoriesList&output=json').json().get(
		'categories')
	categories = [entry['category'] for entry in raw_categories]
	return categories

def getTagList():
	raw_tags = get('http://api.redtube.com/?data=redtube.Tags.getTagList&output=json').json().get('tags')
	tags = [entry['tag']['tag_name'] for entry in raw_tags]
	return tags

def getStarList():
	raw_stars = get('http://api.redtube.com/?data=redtube.Stars.getStarList&output=json').json().get('stars')
	stars = [entry['star']['star_name'] for entry in raw_stars]
	return stars

def getStarDetailedList():
	raw_stars = get('http://api.redtube.com/?data=redtube.Stars.getStarDetailedList&output=json').json().get('stars')
	stars = [(entry['star']['star_name'], entry['star']['star_url']) for entry in raw_stars]
	return stars

def username(url):
	text = get(url).text
	username = re.compile('<td class="withbadge">\\s*<a href="/(?P<username>\\w+)"')
	# likes = re.compile('<span class="percent-likes">(\d\d\d|\d\d|\d)%</span>')
	# views = '<td>102,501\s?<span class="added-time">'
	if username.search(text):
		return username.search(text).group('username')
	return None

class FeedParser(object):
	RSS_FEED = "http://feeds.feedburner.com/redtube/videos"
	TM = attrgetter('tm_year', 'tm_mon', 'tm_mday', 'tm_hour', 'tm_min', 'tm_sec')
	def __init__(self):
		self.feed = feedparser.parse(self.RSS_FEED)
		self.etag = self.feed.get('etag', None)
		self.updated_parsed = self.TM(self.feed.get('updated_parsed')) or None
		self.entries = self.feed.get('entries')

	def get_ids(self):
		ids = [entry.get('id').rsplit('/', 1)[-1] for entry in self.entries]
		return ids

def get_entry(i):
	FEED = feedparser.parse("http://feeds.feedburner.com/redtube/videos").get('entries')
	entry = FEED[i]
	id = int(entry.get('id').rsplit('/', 1)[-1])
	url = entry.get('feedburner_origlink')
	info = get_info(url)
	info['id'] = id
	info['url'] = url
	title = entry.get('title')
	info['title'] = title

	updated_parsed = entry.get('updated_parsed')
	tm = attrgetter('tm_year', 'tm_mon', 'tm_mday', 'tm_hour', 'tm_min', 'tm_sec')(updated_parsed)
	upload_date = datetime(*tm)
	info['upload_date'] = upload_date

	content = entry.get('content')
	summary = entry.get('summary_detail', {}).get('value')
	other = re.compile(
		'\s?Added:\s?(?P<added>\d\d\d\d-\d\d-\d\d),\s?Duration:\s?(?P<duration>\d\d:\d\d|\d:\d\d|:\d\d),\s?Rating:\s(?P<rating>\d.\d\d|\d.\d|\d),\s?Views:\s(?P<views>\d+)')
	gd = other.search(summary).groupdict() if other.search(summary) else {}
	info.update(gd)
	views = int(gd.get('views', '0'))
	info['views'] = views
	rating = gd.get('rating', '0.00')
	info['rating'] = rating
	timing = gd.get('duration', '0:00')
	info['timing'] = timing
	duration = int(sum([x[1] * pow(60, x[0]) for x in enumerate(map(int, timing.split(':')[::-1]))]))
	info['duration'] = duration
	return info
	added_at = isodate.parse_date(gd.get('added', '1900-01-01'))
	link = entry.get('link')
	updated = entry.get('updated')
	updated = isodate.parse_datetime(updated)
	updated_at = attrgetter('year', 'month', 'day', 'hour', 'minute', 'second')(updated)
	updated_at = datetime(*updated_at)
	return info

class Entry(feedparser.FeedParserDict):
	def __init__(self, *args, **kwargs):
		super(Entry, self).__init__(*args, **kwargs)
		self.url = self.get('feedburner_origlink') or self.get('id', '')
		self.title = self.get('title').lower()

	def parse_summary(self):
		from toolz import partition
		self.summary = self.get('summary', ' ').lower()
		title = self.title
		self.unparsed = summary.split(title)[-1].strip('"').strip().replace(',', '')
		unparsed_split = self.unparsed.split()
		if len(unparsed_split) / 2 != 1:
			return self.unparsed
		self._summary = dict(partition(2, unparsed_split))
		return self._summary  # dict(partition(2, unparsed_split))

	def duration(self):
		duration = int(sum([x[1] * pow(60, x[0]) for x in enumerate(map(int, self.duration.split(':')[::-1]))]))
############
def row_is_expanded(self):
	try:
		return bool(self.info)
	except AttributeError:
		return False

def request(data, **kwargs): #todo
	from urllib.parse import urlencode
	ordering = set(['newest', 'mostviewed', 'rating'])
	period = set(['weekly', 'monthly', 'alltime'])
	kwargs.update({'output': 'json', 'data': data})
	url = '%s?%s' % ('http://api.redtube.com/', urlencode(kwargs))
	print(url)
	return get(url).json()

def get_info(url):
	u = G.request(url=url)
	likes = u.select('//span[@class="percent-likes"]').text('0%')
	likes = re.search('(\d\d\d|\d\d|\d)%', likes).group(1)
	tags = '/'.join(u.select('//td[preceding-sibling::td="TAGS"]/a').text_list([])).lower()
	categories = '/'.join(u.select('//td[preceding-sibling::td="FROM"][3]/*').text_list([])).lower()
	stars = '/'.join(u.select('//td[preceding-sibling::td="PORNSTARS"]').text_list([])).lower()
	comments = u.select('//a[@class="comments-btn"]').text('comments (0)')
	comments = re.search('\s?comments\s?\((\d\d\d\d|\d\d\d|\d\d|\d)\)', comments).group(1)
	username = u.select('//td[@class="withbadge"]/a').text('')
	return dict(likes=likes,
	            tags=tags,
	            categories=categories,
	            comments=comments,
	            uploader=username,
	            stars=stars)


#todo
def get_deleted_videos(page=0):
	req = request(data='redtube.Videos.getDeletedVideos', ordering='newest', period='all', page=page)
	return [Row(**entry.get('video')) for entry in req.get('videos')]

def getRedtubeVideos(page=0, category=None):
	http = 'http://api.redtube.com/'
	url = 'http://api.redtube.com/?data=redtube.Videos.searchVideos&output=json&ordering=newest&page={}'.format(page)
	count = get(url).json().get('count')
	videos = get(url).json().get('videos')
	return [Row(RedClient(), **entry.get('video')) for entry in videos]

def getDeletedVideos(page=0):
	page = page
	url = 'http://api.redtube.com/?data=redtube.Videos.getDeletedVideos&ordering=newest&period=alltime&output=json&page={}'.format(
		page)
	json = get(url).json()
	count = json.get('count')
	videos = json.get('videos')
	return dict(query=url, total=count, videos=videos, page=page)

def all_deleted(page=0):
	url = 'http://api.redtube.com/?data=redtube.Videos.getDeletedVideos&ordering=newest&period=alltime&output=json&page={}'.format(
		page=page)
	json = get(url).json()
	count = json.get('count')
