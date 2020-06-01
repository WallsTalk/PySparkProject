#store csv files to HDP, I use scp
scp -P 2222 green_tripdata_2019-01.csv root@<ip_address>:green_tripdata.csv
ssh root@<ip_address> -p 2222

#hadoop script
hadoop fs -mkdir -p /user/root
hadoop fs -mkdir csv_data/green_tripdata
hadoop fs -mkdir csv_data/yellow_tripdata
# when in cd directory
hadoop fs -put green_tripdata.csv csv_data/green_tripdata

