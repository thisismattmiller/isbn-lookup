import sqlite3
import tqdm
import multiprocessing
import time
import requests
import sys, errno

base_path = sys.argv[1]

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
	conn = sqlite3.connect(base_path + 'isbn_data_classify.db', timeout=10)
	write_cursor = conn.cursor()
	for r in add_to_db:
		# print("UPDATING DB!!! ", r['id'])
		write_cursor.execute('UPDATE raw_results set classify = ? WHERE isbn = ?',(r['results'],r['id']))
	conn.commit()
	conn.close()

if __name__ == "__main__":

	log = open(base_path + 'log_classify.txt','a')

	conn = sqlite3.connect(base_path + 'isbn_data_classify.db', timeout=10)
	read_cursor = conn.cursor()

	read_cursor.execute('SELECT count(isbn) FROM raw_results where classify IS NULL')
	total_work_left = int(read_cursor.fetchone()[0])

	read_cursor.execute('SELECT count(isbn) FROM raw_results where classify IS NOT NULL')
	total_work_done = int(read_cursor.fetchone()[0])

	log.write(str(total_work_left) + ' total ISBN left, total ISBNs complete:' +  str(total_work_done) + '\n')
	log.close()

	read_cursor.execute('SELECT count(isbn) FROM raw_results where classify IS NULL')
	total_work = int(read_cursor.fetchone()[0])


	print("Fetching ",total_work,' ISBNs into memory')
	read_cursor.execute('SELECT * FROM raw_results where classify IS NULL')	
	isbns = read_cursor.fetchall()
	conn.close()

	work_counter = 0
	results = []

	lock = multiprocessing.Lock()

	for result in tqdm.tqdm(multiprocessing.Pool(3).imap_unordered(lookup, isbns), total=total_work):	

		if result != None:
			results.append(result)

		if len(results) >= 500:
			lock.acquire()
			add_to_db = results
			results = []
			lock.release()

			update_db(add_to_db)

	update_db(results)

	
