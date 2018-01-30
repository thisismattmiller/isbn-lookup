import sqlite3
import tqdm
import multiprocessing
import time
import requests
import sys, errno

def lookup(data):
	url = 'http://classify.oclc.org/classify2/Classify?isbn=' + str(data[0]) + '&maxRecs=5000'
	try:
		r = requests.get(url)

	except IOError as e:

		print("Error on this one:\n",url)
		# take a little break
		time.sleep(5)
		return None


	if r.text.find('<h1>Too Many Requests</h1>') > -1:
		print("Getting rate limited, pausing this process")
		time.sleep(60)
		return None


	return {"id":data[0],"results":r.text}


def update_db(add_to_db):
	with open('classify_todo1.results','a') as f:
		for x in add_to_db:

			f.write(json.dumps(x) + '\n')


if __name__ == "__main__":


	isbns = []
	with open('classify_todo1') as read:
		for l in read:
			isbns.append(l)

	

	work_counter = 0
	results = []

	lock = multiprocessing.Lock()

	for result in tqdm.tqdm(multiprocessing.Pool(3).imap_unordered(lookup, isbns), total=len(isbns)):	

		if result != None:
			results.append(result)

		if len(results) >= 500:
			lock.acquire()
			add_to_db = results
			results = []
			lock.release()

			update_db(add_to_db)

	update_db(results)

	
