from pyspark.sql.types import *
from pyspark.sql import Row

csv_names = []
csv_names = os.listdir('/users/steponas/Documents/Projects/DataEnginiering/csvData/')
print(csv_names)


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


for key, val in csv_data:
	print(key)
	print(val)

print(csv_headers)



