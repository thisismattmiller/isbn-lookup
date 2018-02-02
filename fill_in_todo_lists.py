import sqlite3
from bs4 import BeautifulSoup
import json 


if __name__ == "__main__":


	conn = sqlite3.connect('isbn_data_classify.db', timeout=100)
	cursor = conn.cursor()
	counter =0
	# for filename in range(6,8):
	# filename = '/Volumes/Byeeee/openlibrary/classify_todo'+str(filename)+'.results'
	filename = '/Volumes/Byeeee/openlibrary/classify_redownload.results'

	print(filename)
	with open(filename,'r') as f:
		for line in f:
			counter+=1
			print(counter, end="\r")
			line = json.loads(line)
			try:
				if isinstance(line['results'], list):
					line['results'] = json.dumps(line['results'])

				cursor.execute('UPDATE raw_results set classify = ? WHERE isbn = ?',(line['results'],line['id']))
			except sqlite3.InterfaceError:
				print("Error updating:",line)

			if counter % 50000 == 0:
				print('commiting', end="\r")
				conn.commit()

	conn.commit()




	# print ("Gathering '2' responses")
	# read_cursor.execute('SELECT * FROM raw_results where classify like \'%<response code="2"/>%\'')

	# for row in read_cursor:

	# 	isbn = row[0]
	# 	xml = row[2]

	# 	soup = BeautifulSoup(xml)

	# 	work_soup = soup.find("work")


	# 	work_author = None if work_soup.has_attr('author') == False else work_soup['author']
	# 	work_editions = None if work_soup.has_attr('editions') == False else work_soup['editions']
	# 	work_eholdings = None if work_soup.has_attr('eholdings') == False else work_soup['eholdings']
	# 	work_format = None if work_soup.has_attr('format') == False else work_soup['format']
	# 	work_holdings = None if work_soup.has_attr('holdings') == False else work_soup['holdings']
	# 	work_itemtype = None if work_soup.has_attr('itemtype') == False else work_soup['itemtype']
	# 	work_owi = None if work_soup.has_attr('owi') == False else work_soup['owi']
	# 	work_title = None if work_soup.has_attr('title') == False else work_soup['title']
	# 	main_oclc = work_soup.text

	# 	print(isbn,work_editions,work_format,work_eholdings)
	# 	authors_soup = soup.find_all("author")
	# 	authors = []
	# 	for a in authors_soup:
	# 		authors.append({
	# 				"name" : a.text,
	# 				"lc" : None if  a.has_attr('lc') == False else a['lc'],
	# 				"viaf" : None if a.has_attr('viaf') == False else a['viaf']
	# 			})
		
	# 	normalized_ddc = None
	# 	normalized_lcc = None
	# 	ddc_soup = soup.find("ddc")
	# 	if ddc_soup:
	# 		ddc_soup = soup.find("ddc").find("mostpopular")
	# 		if ddc_soup.has_attr('nsfa'):
	# 			normalized_ddc = ddc_soup['nsfa']

	# 	lcc_soup = soup.find("lcc")
	# 	if lcc_soup:
	# 		lcc_soup = soup.find("lcc").find("mostpopular")
	# 		if lcc_soup.has_attr('nsfa'):
	# 			normalized_lcc = lcc_soup['nsfa']


	# 	headings = []
	# 	heading_soup = soup.find_all("heading")
	# 	for h in heading_soup:
	# 		headings.append({
	# 				"id" : h['ident'],
	# 				"src": h['src'],
	# 				"value" : h.text
	# 			})
			

	# 	edition_soup = soup.find_all("edition")
	# 	# print(isbn,len(edition_soup))
	# 	for e in edition_soup:
	# 		pass
			

		
		
		# print(isbn,"-------",work_owi)
		# print(soup)

