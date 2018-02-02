import sqlite3
from bs4 import BeautifulSoup
import json 


if __name__ == "__main__":


	conn = sqlite3.connect('isbn_data_classify.db', timeout=10)
	read_cursor = conn.cursor()

	# read_cursor.execute('SELECT count(isbn) FROM raw_results where classify like \'%<response code="2"/>%\'')
	# total_work_done = int(read_cursor.fetchone()[0])



	print ("Gathering '2' responses")
	read_cursor.execute('SELECT * FROM raw_results where classify like \'%<response code="4"/>%\'')

	isbns = []
	counter  = 0
	with open('owis','w') as out:
		for row in read_cursor:
			counter+=1
			isbn = row[0]
			xml = row[2]
			soup = BeautifulSoup(xml)

			work_soup = soup.find_all("work")
			owis = []
			for work in work_soup:

				owis.append(work['owi'])

			out.write(isbn + "|" + ",".join(owis)+'\n')
			# print(isbn + "|" + ",".join(owis))
			if counter % 100 == 0:
				print(counter,'/?')
				# json.dump(owis,open('redownload.json','w'))


	# json.dump(owis,open('multi_response.json','w'))
