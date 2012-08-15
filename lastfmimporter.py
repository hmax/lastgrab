import string
import requests
import redis
import pymongo
import threading, Queue
import signal
import datetime
import sqlite3


from sys import maxint, exit
from time import time, sleep
from types import ListType

con = sqlite3.connect('stat.db', isolation_level=None)
cur = con.cursor() 

r_server = redis.Redis("localhost")
rate = 5.0
per = 1.0


class TokenBucket:
	def __init__(self, rate=5.0, per=1.0):
		self.rate = rate # unit: messages
		self.per = per # unit: seconds
		self.allowance = rate # unit: messages
		self.last_check = time() # floating-point, e.g. usec accuracy. Unit: seconds
	def getToken(self):
		current = time()
		time_passed = current - self.last_check
		self.last_check = current
		self.allowance += time_passed * (self.rate / self.per)
		if (self.allowance > self.rate):
			allowance = rate # throttle
		if (self.allowance < 1.0):
			return False
		else:
			self.allowance = self.allowance - 1.0
			return True


base_uri = "http://ws.audioscrobbler.com/2.0/"
params = {'method' : 'artist.getsimilar', 'artist' : 'cher', 'api_key' : '', 'format' : 'json', 'limit' : 99999999}


globalBucket = TokenBucket(rate, per)

def starting_seed():

	for letter in string.ascii_lowercase:
		params['method'] = 'artist.search'
		params['artist'] = letter
		params['limit'] = maxint
		while True:
			if globalBucket.getToken():
				try:
					r = requests.get(base_uri, params=params)
					if r.status_code == 200:
						if r.json:
							for artist in r.json['results']['artistmatches']['artist']:
								if not r_server.sismember('to_visit', artist['name']):
									r_server.sadd('to_visit', artist['name'])
							break
				except requests.exceptions.Timeout as t:
					print('timeout while searching for {0}'.format(letter))
			else:
				print('Sleeping')
				sleep(per/2)

m_server = pymongo.Connection()


class Grabber(threading.Thread):
	def __init__(self, queue, r_server, m_server):
		self.r_server = r_server
		self.m_similars = m_server['lastfm']['artist']
		self.params = {'method' : 'artist.getsimilar', 'api_key' : '919ce0f536caf75bd1b732bfc1bbeb36', 'format' : 'json', 'limit' : 99999}
		self._queue = queue

		threading.Thread.__init__(self)

	def run(self):
		while 1:
			artist_name = self._queue.get()
			similars = self.getArtistSimilars(artist_name)
			print("Trying {0}".format(artist_name))
			if similars:
				filtered_similars = []
				if isinstance(similars['similarartists']['artist'], ListType):
					for artist in similars['similarartists']['artist']:
						similar = {}
						similar['n'] = artist['name']
						similar['mb'] = artist['mbid']
						similar['m'] = float(artist['match'])
						filtered_similars.append(similar)
				else:
					similar = {}
					if ('name' not in artist) or ('mbid' not in artist) or ('match' not in artist):
						break
					similar['n'] = artist['name']
					similar['mb'] = artist['mbid']
					similar['m'] = float(artist['match'])
					filtered_similars.append(similar)

				self.m_similars.insert({'n' : artist_name, 's' : filtered_similars})
				for artist in filtered_similars:
					if not self.r_server.sismember('visited', artist['n']):
						self.r_server.sadd('to_visit', artist['n'])
				self.r_server.srem('to_visit', artist_name)
				self.r_server.sadd('visited', artist_name)
				self._queue.task_done()
			else:
				self._queue.put(artist_name)

	def getArtistSimilars(self, artist):
		self.params['artist'] = artist
		try:
			r = requests.get(base_uri, params=self.params)
			if r.status_code == 200:
				return r.json
		except requests.exceptions.Timeout as t:
			print('timeout while searching for {0}'.format(artist))
			return None



queue = Queue.Queue(0)
for i in range(10):
    Grabber(queue, r_server, m_server).start() # start a worker

def sighandler(signum, frame):
	print('Quitting')
	queue.join()
	exit(0)


signal.signal(signal.SIGINT, sighandler)


last_check = time()

while r_server.scard('to_visit') > 0:
	current = time()
	if current - last_check >= 30:
		cur.execute("INSERT INTO stats VALUES (?,?,?)", (datetime.datetime.now(), r_server.scard('to_visit'), r_server.scard('visited')))
		last_check = current
	
	if globalBucket.getToken():
		artist_name = r_server.srandmember('to_visit')
		queue.put(artist_name)
	else:
		sleep(per/10)


