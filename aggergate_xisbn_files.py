import glob, json

counter = 0
with open('/Volumes/Byeeee/xisbn/loc_files.ndjson', 'w') as out:
	for filename in glob.iglob('/Users/thisismattmiller/Downloads/home/judec/xisbn/loc_files/*.json'):
		counter+=1
		if counter % 1000 == 0:
			print(counter,filename)

		try:
			data = json.load(open(filename))
			out.write(json.dumps(data) + '\n')
		except:
			print("problem with this file",filename)

