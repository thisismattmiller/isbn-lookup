import sqlite3
import tqdm
import multiprocessing
import time
import requests
import sys, errno
import json
import os

filename = sys.argv[1]


def lookup(data):
	url = 'http://classify.oclc.org/classify2/Classify?isbn=' + str(data) + '&maxRecs=5000'
	try:
		r = requests.get(url, headers={'Connection':'close'})

	except IOError as e:

		print("Error on this one:\n",url)
		# take a little break
		time.sleep(1)

		#try again
		try:
			r = requests.get(url, headers={'Connection':'close'})
			return {"id":data,"results":r.text}
		except IOError as e:
			print("2nd Error on this one:\n",url)
			time.sleep(1)
			try:
				r = requests.get(url, headers={'Connection':'close'})
				return {"id":data,"results":r.text}
			except IOError as e:
				print("Final Error on this one:\n",url)
				return None

		


	if r.text.find('<h1>Too Many Requests</h1>') > -1:
		print("Getting rate limited, pausing this process")
		time.sleep(30)
		return None

	
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


	# for result in tqdm.tqdm(multiprocessing.Pool(3).imap_unordered(lookup, isbns), total=len(isbns)):	
	for isbn in isbns:

		work_counter += 1

		print(str(work_counter) + '/' + str(len(isbns)))
		result = lookup(isbn)
		print(result)

		if result != None:
			results.append(result)

		if len(results) >= 50:
			add_to_db = results
			results = []

			update_db(add_to_db)

	update_db(results)

	
