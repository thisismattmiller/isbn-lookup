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

	isbn = data.split('|')[0]
	owis = data.split('|')[1].split(',')
	results = {"id":isbn,"results":[],"multi":True}



	for owi in owis:



		url = 'http://classify.oclc.org/classify2/Classify?owi=' + str(owi) + '&maxRecs=100'


		try:

			r = requests.get(url, headers={'Connection':'close'})

			results['results'].append(r.text)
			
			page = re.search(r'\<next\>([0-9]+)\<\/next\>', str(r.text),re.M)
			if page:
				
				while page:	
					url = 'http://classify.oclc.org/classify2/Classify?owi=' + str(owi) + '&maxRecs=100&startRec='+page.group(1)
					print("Multiple page of editions:",url)
					try:

						r = requests.get(url, headers={'Connection':'close'})

						if r.text.find('<h1>Too Many Requests</h1>') > -1:
							print("Getting rate limited while downloading editions")
							time.sleep(10)
							return None		
						
					except IOError as e:
						print("Broke trying to get multiple editions")
						return None


					results['results'].append(r.text)
					page = re.search(r'\<next\>([0-9]+)\<\/next\>', str(r.text),re.M)




		except IOError as e:

			print("Error on this one:\n",url)
			# take a little break
			time.sleep(1)



		if r.text.find('<h1>Too Many Requests</h1>') > -1:
			print("Getting rate limited, pausing this process")
			time.sleep(30)
			return None

	
	return results


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


	for result in tqdm.tqdm(multiprocessing.Pool(3).imap_unordered(lookup, isbns), total=len(isbns)):	


		# print(str(work_counter) + '/' + str(len(isbns)))

		if result != None:
			results.append(result)

		if len(results) >= 50:
			lock.acquire()
			add_to_db = results
			results = []
			lock.release()

			update_db(add_to_db)

	update_db(results)

	
