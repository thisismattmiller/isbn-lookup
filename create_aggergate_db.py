import sqlite3
conn = sqlite3.connect('isbn_data.db', check_same_thread = False)


if __name__ == "__main__":

	c = conn.cursor()

	c.execute('''CREATE TABLE IF NOT EXISTS data
	             (isbn text PRIMARY KEY UNIQUE, 
	             has_xisbn BOOLEAN DEFAULT 0,
	             has_classify BOOLEAN DEFAULT 0,
	             has_ai BOOLEAN DEFAULT 0,
	             has_google BOOLEAN DEFAULT 0,
	             classify_author text DEFAULT null,
	             classify_edition_count INTEGER DEFAULT null,
	             classify_ehodings_count INTEGER DEFAULT null,
	             classify_holdings_count INTEGER DEFAULT null,
	             classify_item_type text DEFAULT null,
	             classify_work_id text DEFAULT null, 
	             classify_title text DEFAULT null,
	             classify_dewey text DEFAULT null,
	             classify_lcc text DEFAULT null,
	             classify_fast_subject_headings text DEFAULT null, 
	             classify_editions text DEFAULT null, 
	             xisbn_oclc_id text DEFAULT null,
	             xisbn_title text DEFAULT null,
	             xisbn_year text DEFAULT null,
	             xisbn_language text DEFAULT null,
	             xisbn_edition text DEFAULT null,
	             xisbn_author text DEFAULT null,
	             xisbn_publisher text DEFAULT null,
	             xisbn_city text DEFAULT null,
	             xisbn_form text DEFAULT null,
	             ia_books_id text DEFAULT null,
	             ia_works_id text DEFAULT null,
	             ia_publishers text DEFAULT null,
	             ia_physical_format text DEFAULT null,
	             ia_number_of_pages text DEFAULT null,
	             ia_publish_date text DEFAULT null,
	             ia_title text DEFAULT null,
	             ia_authors text DEFAULT null,
	             google_title text DEFAULT null,
	             google_authors text DEFAULT null,
	             google_publisher text DEFAULT null,
	             google_publisher_date text DEFAULT null,
	             google_description text DEFAULT null,
	             google_page_count text DEFAULT null,
	             google_print_type text DEFAULT null,
	             google_language text DEFAULT null
	             );''')

	# classify_author text, # list, [{"name":"xyz","viaf":123}...]
	# classify_work_id text, # list [12345,54321]
	# classify_fast_subject_headings text, # list [{"term":"abc","fast_id":123}...]
	# classify_editions text, # list [{"oclc_id":1234,"title":"abc","language":"eng","form":"book"}...]
	# xisbn_oclc_id text, # list [1234,4321,...]

	count = 0
	with open("woo-llc10.txt") as f:

		for isbn in f:

			count += 1
			if count % 10000 == 0:
				print(count)
				conn.commit()

			try:
				c.execute('INSERT INTO data(isbn) VALUES(?)', [isbn.strip()])
			except sqlite3.IntegrityError:
				pass

	conn.close()

