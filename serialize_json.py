import sqlite3
import json 


def to_obj(value):
	try:
		value = json.loads(value)
	except TypeError:
		value = None

	return value

if __name__ == "__main__":


	conn = sqlite3.connect('isbn_data.db', timeout=10)
	conn.row_factory = sqlite3.Row
	read_cursor = conn.cursor()

	read_cursor.execute('SELECT * FROM data where has_xisbn = 1 and has_classify = 1 and has_ai = 1')
	counter = 0
	for row in read_cursor:

		ia_authors = []
		for x in to_obj(row['ia_authors']):

			ia_authors.append(x['key'])

		if row['xisbn_form'] == None:
			continue 

		output = {
			"isbn" : row['isbn'],
			"classify_author" : to_obj(row['classify_author']),
			"classify_work_id" : to_obj(row['classify_work_id']),
			"classify_fast_subject_headings" : to_obj(row['classify_fast_subject_headings']),
			"classify_editions" : to_obj(row['classify_editions']),
			"ia_works_id" : to_obj(row['ia_works_id']),
			"ia_publishers" : to_obj(row['ia_publishers']),
			"ia_authors" : ia_authors,
			"xisbn_oclc_id" : to_obj(row['xisbn_oclc_id']),
			"xisbn_form" : to_obj(row['xisbn_form']),

			"has_xisbn" : row['has_xisbn'],
			"has_classify" : row['has_classify'],
			"has_ia" : row['has_ai'],
			"has_google" : row['has_google'],
			"classify_edition_count" : row['classify_edition_count'],
			"classify_holdings_count" : row['classify_holdings_count'],
			"classify_ehodings_count" : row['classify_ehodings_count'],
			"classify_item_type" : row['classify_item_type'],
			"classify_title" : row['classify_title'],
			"classify_dewey" : row['classify_dewey'],
			"classify_lcc" : row['classify_lcc'],

			"xisbn_title" : row['xisbn_title'],
			"xisbn_year" : row['xisbn_year'],
			"xisbn_language" : row['xisbn_language'],
			"xisbn_edition" : row['xisbn_edition'],
			"xisbn_author" : row['xisbn_author'],
			"xisbn_publisher" : row['xisbn_publisher'],
			"xisbn_city" : row['xisbn_city'],

			"ia_books_id" : row['ia_books_id'],
			"ia_number_of_pages" : row['ia_number_of_pages'],
			"ia_publish_date" : row['ia_publish_date'],
			"ia_title" : row['ia_title'],





		}
		counter+=1
		print(counter)
		json.dump(output,open('examples/'+row['isbn']+'.json','w'),sort_keys=True,indent=2)
