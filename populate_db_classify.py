import sqlite3
from bs4 import BeautifulSoup
import json 




def extract_classify(xml):


		soup = BeautifulSoup(str(xml))

		work_soup = soup.find("work")

		results = {}

		results['work_author'] = None if work_soup.has_attr('author') == False else work_soup['author']
		results['work_editions'] = None if work_soup.has_attr('editions') == False else int(work_soup['editions'])
		results['work_eholdings'] = None if work_soup.has_attr('eholdings') == False else int(work_soup['eholdings'])
		results['work_format'] = None if work_soup.has_attr('format') == False else work_soup['format']
		results['work_holdings'] = None if work_soup.has_attr('holdings') == False else int(work_soup['holdings'])
		results['work_itemtype'] = None if work_soup.has_attr('itemtype') == False else work_soup['itemtype']
		results['work_owi'] = None if work_soup.has_attr('owi') == False else work_soup['owi']
		results['work_title'] = None if work_soup.has_attr('title') == False else work_soup['title']
		results['main_oclc'] = work_soup.text


		authors_soup = soup.find_all("author")
		results['authors'] = []
		for a in authors_soup:
			results['authors'].append({
					"name" : a.text,
					"lc" : None if  a.has_attr('lc') == False else a['lc'],
					"viaf" : None if a.has_attr('viaf') == False else a['viaf']
				})
		

		results["normalized_ddc"] = None
		results["normalized_lcc"] = None

		ddc_soup = soup.find("ddc")
		if ddc_soup:
			ddc_soup = soup.find("ddc").find("mostpopular")
			if ddc_soup.has_attr('nsfa'):
				results["normalized_ddc"] = ddc_soup['nsfa']

		lcc_soup = soup.find("lcc")
		if lcc_soup:
			lcc_soup = soup.find("lcc").find("mostpopular")
			if lcc_soup.has_attr('nsfa'):
				results["normalized_lcc"] = lcc_soup['nsfa']


		results["headings"] = []
		heading_soup = soup.find_all("heading")
		for h in heading_soup:
			results["headings"].append({
					"id" : h['ident'],
					"src": h['src'],
					"value" : h.text
				})
			

		edition_soup = soup.find_all("edition")
		# print(isbn,len(edition_soup))
		results["editions"] = []
		for e in edition_soup:
			edition = {}
			edition['author'] = None if e.has_attr('author') == False else e['author']
			edition['eholdings'] = None if e.has_attr('eholdings') == False else int(e['eholdings'])
			edition['format'] = None if e.has_attr('format') == False else e['format']
			edition['holdings'] = None if e.has_attr('holdings') == False else int(e['holdings'])
			edition['itemtype'] = None if e.has_attr('itemtype') == False else e['itemtype']
			edition['language'] = None if e.has_attr('language') == False else e['language']
			edition['oclc'] = None if e.has_attr('oclc') == False else e['oclc']
			edition['title'] = None if e.has_attr('title') == False else e['title']
			results["editions"].append(edition)
			
		return results

def write_data(data,cursor):




	write_cursor.execute('''UPDATE data set 
		has_classify = ?, 
		classify_author = ?,
		classify_edition_count = ?,
		classify_ehodings_count = ?,
		classify_holdings_count = ?,
		classify_item_type = ?,
		classify_work_id = ?,
		classify_title = ?,
		classify_dewey = ?,
		classify_lcc = ?,
		classify_editions = ?,
		classify_fast_subject_headings = ?
		where isbn = ?''',[
			1,
			json.dumps(data['authors']),
			data['work_editions'],
			data['work_eholdings'],
			data['work_holdings'],
			data['work_itemtype'],
			json.dumps(data['work_owi']),
			data['work_title'],
			data['normalized_ddc'],
			data['normalized_lcc'],
			json.dumps(data['editions']),
			json.dumps(data['headings']),
			isbn
			])








if __name__ == "__main__":


	conn = sqlite3.connect('isbn_data_classify.db', timeout=10)
	read_cursor = conn.cursor()

	conn2 = sqlite3.connect('isbn_data.db', timeout=10)
	write_cursor = conn2.cursor()


	read_cursor.execute('SELECT * FROM raw_results where classify IS NOT NULL')
	counter = 0
	for row in read_cursor:

		isbn = row[0]

		xml = str(row[2])
		xml_multi = row[3]

		if xml.find('<response code="2"/>') > -1 or xml.find('<response code=\\"2\\"/>') >-1:
			counter+=1

			
			# single response, but may have multiple pages of editions
			if xml[0] == '<':
				results = extract_classify(xml)
				write_data(results,write_cursor)

				pass
			# multi response
			elif xml[0] == '[':

				editions = []
				results = None
				for xml in json.loads(xml):
					results = extract_classify(xml)
					editions = editions + results['editions']

				# overwrite the last ediitons
				results['editions'] = editions
				# make owi a list
				results['work_owi'] = [owis]
				write_data(results,write_cursor)
			else:
				print('2 Problem',xml)

			# write_data(results,write_cursor)

		# multi response
		elif xml.find('<response code="4"/>') > -1:
			counter+=1


			# find the one with the largest holdings
			# print(isbn)
			editions = []
			owis = []
			largest_result = None
			largest_result_count = 0
			for xml in json.loads(xml_multi):
				results = extract_classify(xml)
				editions = editions + results['editions']
				owis.append(results['work_owi'])
				if results['work_holdings'] > largest_result_count:
					largest_result = results
			
			# overwrite the last ediitons
			results['editions'] = editions
			# make owi a list
			results['work_owi'] = owis
			write_data(results,write_cursor)


			pass
		elif xml.find('<response code="100"/>') > -1 or xml.find('<response code=\\"100\\"/>') >-1:
			print(isbn,'100: No input. The method requires an input argument.')
		elif xml.find('<response code="101"/>') > -1 or xml.find('<response code=\\"101\\"/>') >-1:
			print(isbn,'101: Invalid input. The standard number argument is invalid.')
		elif xml.find('<response code="102"/>') > -1 or xml.find('<response code=\\"102\\"/>') >-1:
			pass
		elif xml.find('<response code="200"/>') > -1 or xml.find('<response code=\\"200\\"/>') >-1:
			print(isbn,'200: Unexpected error.')
		else:

			print(xml.find('<response code=\"2\"/>'))
			print("unknown Problem:",xml)



		if counter % 10000 == 0:
			print(counter)
			conn2.commit()

	conn2.commit()



