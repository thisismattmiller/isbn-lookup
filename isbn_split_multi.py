import sqlite3
import tqdm
import multiprocessing
import time
import requests
import sys, errno
import json
import os
import re

filename = sys.argv[1]


def lookup(data):

	url = 'https://isbndb.com/book/' + str(data) 

	try:

		r = requests.get(url, headers={'Connection':'close', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'})


	except IOError as e:

		print("Error on this one:\n",url)
		# take a little break
		time.sleep(1)

		#try again
		try:
			r = requests.get(url, headers={'Connection':'close', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'})
			return {"id":data,"results":r.text}
		except IOError as e:
			print("2nd Error on this one:\n",url)
			time.sleep(1)
			try:
				r = requests.get(url, headers={'Connection':'close', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'})
				return {"id":data,"results":r.text}
			except IOError as e:
				print("Final Error on this one:\n",url)
				return None

	
	print(r.status_code)

	
	
	return {"id":data,"results":r.text}


def update_db(add_to_db):
	with open(filename+'.results','a') as f:
		for x in add_to_db:

			f.write(json.dumps(x) + '\n')


if __name__ == "__main__":


	isbns = []
	compelted_isbns = {}
	# try to load the .result file first to see if there is anything there
	if os.path.isfile(filename + '.results'):
		with open(filename+ '.results') as read:
			for l in read:
				d = json.loads(l)
				compelted_isbns[d['id'].strip()] = True

	print(len(compelted_isbns),'already compelted')

	with open(filename) as read:
		for l in read:
			if l.strip() not in compelted_isbns:
				isbns.append(l.strip())


	print(len(isbns),' ready to work')

	work_counter = 0
	results = []

	lock = multiprocessing.Lock()


	for result in tqdm.tqdm(multiprocessing.Pool(2).imap_unordered(lookup, isbns), total=len(isbns)):	


		# print(str(work_counter) + '/' + str(len(isbns)))

		if result != None:
			results.append(result)

		if len(results) >= 10:
			lock.acquire()
			add_to_db = results
			results = []
			lock.release()

			update_db(add_to_db)

	update_db(results)

	
