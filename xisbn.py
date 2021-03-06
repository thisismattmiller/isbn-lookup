import sqlite3
import tqdm
import multiprocessing
import time
import requests
import sys, errno

base_path = sys.argv[1]

def lookup(data):
	url = 'http://xisbn.worldcat.org/webservices/xid/isbn/' + str(data[0])  + '?method=getEditions&format=json&fl=*'
	try:
		r = requests.get(url)

	except IOError as e:

		if e.errno == errno.EPIPE:
			print("Error on this one:\n",url)
			# take a little break
			time.sleep(5)
			return None

	if r.text.find('"stat":"overlimit"') > -1:
		print(data[0],"overlimit!")
		return None

	return {"id":data[0],"results":r.text}


def update_db(add_to_db):
	conn = sqlite3.connect(base_path + 'isbn_data.db', timeout=10)
	write_cursor = conn.cursor()
	for r in add_to_db:
		# print("UPDATING DB!!! ", r['id'])
		write_cursor.execute('UPDATE raw_results set xisbn = ? WHERE isbn = ?',(r['results'],r['id']))
	conn.commit()
	conn.close()

if __name__ == "__main__":

	log = open(base_path + 'log.txt','a')

	conn = sqlite3.connect(base_path + 'isbn_data.db', timeout=10)
	read_cursor = conn.cursor()

	read_cursor.execute('SELECT count(isbn) FROM raw_results where xisbn IS NULL')
	total_work_left = int(read_cursor.fetchone()[0])

	read_cursor.execute('SELECT count(isbn) FROM raw_results where xisbn IS NOT NULL')
	total_work_done = int(read_cursor.fetchone()[0])

	log.write(str(total_work_left) + ' total ISBN left, total ISBNs complete:' +  str(total_work_done) + '\n')
	log.close()

	read_cursor.execute('SELECT count(isbn) FROM raw_results where xisbn IS NULL LIMIT 1000')
	total_work = int(read_cursor.fetchone()[0])


	print("Fetching ",total_work,' ISBNs into memory')
	read_cursor.execute('SELECT * FROM raw_results where xisbn IS NULL LIMIT 1000')	
	isbns = read_cursor.fetchall()
	conn.close()

	work_counter = 0
	results = []

	lock = multiprocessing.Lock()

	# for result in multiprocessing.Pool(5).imap(lookup, isbns):
	for result in tqdm.tqdm(multiprocessing.Pool(5).imap_unordered(lookup, isbns), total=total_work):	

		if results != None:
			results.append(result)

		if len(results) >= 500:
			lock.acquire()
			add_to_db = results
			results = []
			lock.release()

			update_db(add_to_db)

	update_db(results)

	