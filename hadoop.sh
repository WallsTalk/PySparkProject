#store csv files to HDP, I use scp
scp -P 2222 greentest.csv root@192.168.8.102:greentest.csv
ssh root@<ip_address> -p 2222

#hadoop script
hadoop fs -mkdir -p /user/hive
hadoop fs -mkdir /user/hive/csv_data/green_tripdata
hadoop fs -mkdir /user/hive/csv_data/yellow_tripdata
# when in cd directory
hadoop fs -put greentest.csv /user/hive/csv_data/green_tripdata

