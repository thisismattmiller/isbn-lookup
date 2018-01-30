import sqlite3
conn = sqlite3.connect('isbn_data.db', check_same_thread = False)


if __name__ == "__main__":

	c = conn.cursor()
	c.execute('''CREATE TABLE IF NOT EXISTS raw_results
	             (isbn text PRIMARY KEY UNIQUE, xisbn text, classify text)''')
	count = 0
	with open("woo-llc10.txt") as f:

		for isbn in f:

			count += 1
			if count % 10000 == 0:
				print(count)
				conn.commit()

			try:
				c.execute('INSERT INTO raw_results VALUES (?,?,?)', (isbn.strip(), None, None))				
			except sqlite3.IntegrityError:
				pass

	conn.close()

