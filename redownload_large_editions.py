import sqlite3
from bs4 import BeautifulSoup
import json 


if __name__ == "__main__":


	conn = sqlite3.connect('isbn_data_classify.db', timeout=10)
	read_cursor = conn.cursor()

	# read_cursor.execute('SELECT count(isbn) FROM raw_results where classify like \'%<response code="2"/>%\'')
	# total_work_done = int(read_cursor.fetchone()[0])



	print ("Gathering '2' responses",total_work_done)
	read_cursor.execute('SELECT * FROM raw_results where classify like \'%<response code="2"/>%\'')

	isbns = []
	counter  = 0
	for row in read_cursor:
		counter+=1
		isbn = row[0]
		xml = row[2]
		soup = BeautifulSoup(xml)

		work_soup = soup.find("work")
		work_editions = None if work_soup.has_attr('editions') == False else work_soup['editions']			
		edition_soup = len(soup.find_all("edition"))
		

		if (int(edition_soup) < int(work_editions)):
			isbns.append(isbn)


			if len(isbns) % 100 == 0:
				print(len(isbns),counter,'/',total_work_done)
				# json.dump(isbns,open('redownload.json','w'))


	json.dump(isbns,open('redownload.json','w'))
