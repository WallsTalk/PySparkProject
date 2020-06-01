CREATE SCHEMA IF NOT EXISTS taxidata;

CREATE EXTERNAL TABLE IF NOT EXISTS taxidata.green
(VendorID int,lpeppickupdatetime string,lpepdropoffdatetime string,storendfwdflag string,RatecodeID string,PULocationID string,DOLocationID int,passenger_count string,trip_distance string,fare_amount string,extra string,mtatax string,tip_amount string,tolls_amount string,ehail_fee string,improvement_surcharge string,total_amount string,payment_type string,trip_type string,congestion_surcharge string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/csv_data/green_tripdata';
