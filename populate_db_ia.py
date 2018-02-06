import sqlite3
import json 





if __name__ == "__main__":




	conn2 = sqlite3.connect('isbn_data.db', timeout=10)
	write_cursor = conn2.cursor()
	counter = 0
	with open('/Volumes/Byeeee/openlibrary/ol_dump_editions_latest.txt') as file:

		for line in file:

			data = line.split('\t')
			ia_id = data[1]
			json_data = json.loads(data[4])

			works = []


			json_data['works'] = [] if 'works' not in json_data else json_data['works']
			json_data['publishers'] = [] if 'publishers' not in json_data else json_data['publishers']
			json_data['physical_format'] = None if 'physical_format' not in json_data else json_data['physical_format']
			json_data['number_of_pages'] = None if 'number_of_pages' not in json_data else json_data['number_of_pages']
			json_data['publish_date'] = None if 'publish_date' not in json_data else json_data['publish_date']
			json_data['title'] = None if 'title' not in json_data else json_data['title']
			json_data['authors'] = [] if 'authors' not in json_data else json_data['authors']


			for x in json_data['works']:
				works.append(x['key'])

			if 'isbn_13' not in json_data:
				continue


			for isbn in json_data['isbn_13']:
				write_cursor.execute('''UPDATE data set 
					has_ai = ?, 
					ia_books_id = ?,
					ia_works_id = ?,
					ia_publishers = ?,
					ia_physical_format = ?,
					ia_number_of_pages = ?,
					ia_publish_date = ?,
					ia_title = ?,
					ia_authors = ?
					where isbn = ?''',[
						1,
						ia_id,
						json.dumps(works),
						json.dumps(json_data['publishers']),
						json_data['physical_format'],
						json_data['number_of_pages'],
						json_data['publish_date'],
						json_data['title'],
						json.dumps(json_data['authors']),
						isbn
						])


				if write_cursor.rowcount > 0:
					counter += 1 

			

			if counter % 10000 == 0:
				print(counter)
				counter += 1 
				conn2.commit()



	conn2.commit()



