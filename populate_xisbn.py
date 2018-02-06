import sqlite3
import json 
from isbntools.app import *

if __name__ == "__main__":


	conn = sqlite3.connect('isbn_data.db', timeout=10)
	read_cursor = conn.cursor()
	write_cursor = conn.cursor()


	files = ['/Volumes/Byeeee/xisbn/vinay_wishlist_jan2018_files.ndjson', '/Volumes/Byeeee/xisbn/raw_isbn_files.ndjson','/Volumes/Byeeee/xisbn/mek_files.ndjson','/Volumes/Byeeee/xisbn/95pct-isbn-score_files.ndjson','/Volumes/Byeeee/xisbn/loc_files.ndjson']
	for file in files:

		counter = 0
		with open(file, 'r') as file:
			for line in file:
				line = json.loads(line)
				for item in line['list']:
					for isbn in item['isbn']:
						if len(isbn) == 10:
							isbn = to_isbn13(isbn)

						if 'form' not in item:
							# missing basic stuff we are interested in
							continue


	             # xisbn_oclc_id text DEFAULT null,
	             # xisbn_title text DEFAULT null,
	             # xisbn_year text DEFAULT null,
	             # xisbn_language text DEFAULT null,
	             # xisbn_edition text DEFAULT null,
	             # xisbn_author text DEFAULT null,
	             # xisbn_publisher text DEFAULT null,
	             # xisbn_city text DEFAULT null,
						

						item['author'] = None if 'author' not in item else item['author']
						item['oclcnum'] = [] if 'oclcnum' not in item else item['oclcnum']
						item['title'] = None if 'title' not in item else item['title']
						item['year'] = None if 'year' not in item else item['year']
						item['lang'] = None if 'lang' not in item else item['lang']
						item['ed'] = None if 'ed' not in item else item['ed']
						item['publisher'] = None if 'publisher' not in item else item['publisher']
						item['city'] = None if 'city' not in item else item['city']
						item['form'] = None if 'form' not in item else item['form']




						write_cursor.execute('''UPDATE data set 
							has_xisbn = ?, 
							xisbn_oclc_id = ?,
							xisbn_title = ?,
							xisbn_year = ?,
							xisbn_language = ?,
							xisbn_edition = ?,
							xisbn_author = ?,
							xisbn_publisher = ?,
							xisbn_city = ?,
							xisbn_form = ?

							where isbn = ?''',[
								1,
								json.dumps(item['oclcnum']),
								item['title'],
								item['year'],
								item['lang'],
								item['ed'],
								item['author'],
								item['publisher'],
								item['city'],
								json.dumps(item['form']),
								isbn
								])

						if write_cursor.rowcount > 0:
							counter += 1 

						if counter % 10000 == 0:
							conn.commit()
							print(counter)
							counter+=1




	# counter = 0
	# with open('/Volumes/Byeeee/xisbn/raw_isbn_files.ndjson', 'r') as file:
	# 	for line in file:
	# 		line = json.loads(line)
	# 		for item in line['list']:
	# 			for isbn in item['isbn']:
	# 				if len(isbn) == 10:
	# 					isbn = to_isbn13(isbn)

	# 				write_cursor.execute('UPDATE data set has_xisbn = ? where isbn = ?',[1,isbn])
	# 				counter += 1 

	# 				if counter % 10000 == 0:
	# 					conn.commit()
	# 					print(counter, isbn)


	# counter = 0
	# with open('/Volumes/Byeeee/xisbn/mek_files.ndjson', 'r') as file:
	# 	for line in file:
	# 		line = json.loads(line)
	# 		for item in line['list']:
	# 			for isbn in item['isbn']:
	# 				if len(isbn) == 10:
	# 					isbn = to_isbn13(isbn)

	# 				write_cursor.execute('UPDATE data set has_xisbn = ? where isbn = ?',[1,isbn])
	# 				counter += 1 

	# 				if counter % 10000 == 0:
	# 					conn.commit()
	# 					print(counter, isbn)



	# counter = 0
	# with open('/Volumes/Byeeee/xisbn/95pct-isbn-score_files.ndjson', 'r') as file:
	# 	for line in file:
	# 		line = json.loads(line)
	# 		for item in line['list']:
	# 			for isbn in item['isbn']:
	# 				if len(isbn) == 10:
	# 					isbn = to_isbn13(isbn)

	# 				write_cursor.execute('UPDATE data set has_xisbn = ? where isbn = ?',[1,isbn])
	# 				counter += 1 

	# 				if counter % 10000 == 0:
	# 					conn.commit()
	# 					print(counter, isbn)


	counter = 0
	with open('/Volumes/Byeeee/xisbn/loc_files.ndjson', 'r') as file:
		for line in file:
			line = json.loads(line)
			for item in line['list']:
				for isbn in item['isbn']:
					if len(isbn) == 10:
						isbn = to_isbn13(isbn)

					write_cursor.execute('UPDATE data set has_xisbn = ? where isbn = ?',[1,isbn])
					counter += 1 

					if counter % 10000 == 0:
						conn.commit()
						print(counter, isbn)


	

