#!/usr/bin/env python3
#from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import SparkContext
import re


#taking all the file names from directory with all the csv files
csv_names = []
csv_names = os.listdir('/users/steponas/Documents/Projects/DataEnginiering/csvData/')
print(csv_names)


#creating list of headers and dictionary with key(unique_header):csv_data 

csv_headers = []
csv_data = {}
for item in csv_names:
	csv_name = sc.textFile("/users/steponas/Documents/Projects/DataEnginiering/csvData/" + item)
	try: temp = csv_name.map(lambda p: p.split(","))	
	except:
		print("couldn't format the" + str(csv_name))
		continue
	try: str(temp.first())	
	except:
		print("couldn't cast to string " + str(csv_name))
		continue
	header = temp.first()
	if str(header) in csv_headers:
		temp = temp.filter(lambda p:p != header)
		csv_data[str(header)] = csv_data[str(header)].union(temp)
	else:
		csv_headers.append(str(temp.first()))
		temp = temp.filter(lambda p:p != header)
		csv_data[str(header)] = temp

#making a list of dataframes for eatch unique key(unique_header):csv_data 
#common error when casting to tables (contains invalid character(s) among " ,;{}()\n\t=")
dfList = []
for key in csv_data:
	count = 0
	header = re.sub("[][ '']", '', key).split(",")  
	header_row = Row(*header)
	dfList.append(csv_data[key].map(lambda p: header_row(*p[:len(header)])).toDF())
	count += 1

#prepareing data for sending to HDF
#for frame in dfList:
#	dfList[dfList.index(frame)].write.format("csv").save("/users/steponas/Documents/Projects/DataEnginiering/output")



