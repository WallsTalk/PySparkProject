from pyspark.sql.types import *
from pyspark.sql import Row

#taking all the file names from directory with all the csv files
csv_names = []
csv_names = os.listdir('/users/steponas/Documents/Projects/DataEnginiering/csvData/')
print(csv_names)


#creating list of headers and dictionary with key(unique_header):csv_data 
csv_headers = []
csv_data = {}
for item in csv_names:
	csv_name = sc.textFile("/users/steponas/Documents/Projects/DataEnginiering/csvData/" + item)
	try:
		temp = csv_name.map(lambda p: p.split(","))
	except:
		continue
	if str(temp.first()) in csv_headers:
			csv_data[str(temp.first())] = csv_data[str(temp.first())].union(temp)
			temp = temp.filter(lambda p:p != header)
	else:
		csv_headers.append(str(temp.first()))
		csv_data[str(temp.first())] = temp


#making a list of dataframes for eatch unique key(unique_header):csv_data 
dfList = []
for key in csv_data:
	count = 0
	header = key.split(",")
	header_dict = {}
	print(len(header))
	#p = []
	for item in header:
		print(item)
		if header.index(item) == 0:
			header_dict[item] = int(1)
		else:
			header_dict[item] = 1
	dfList.append(csv_data[key].map(lambda p: Row(**header_dict)).toDF())
	count += 1







