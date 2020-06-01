CREATE SCHEMA IF NOT EXISTS taxidata;

CREATE EXTERNAL TABLE IF NOT EXISTS taxidata.green
(VendorID int,lpeppickupdatetime string,lpepdropoffdatetime string,storendfwdflag string,RatecodeID string,PULocationID string,DOLocationID int,passenger_count string,trip_distance string,fare_amount string,extra string,mtatax string,tip_amount string,tolls_amount string,ehail_fee string,improvement_surcharge string,total_amount string,payment_type string,trip_type string,congestion_surcharge string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/csv_data/green_tripdata';


CREATE EXTERNAL TABLE IF NOT EXISTS taxidata.yellow
(VendorID int,tpep_pickup_datetime string,tpep_dropoff_datetime string,passenger_count int,trip_distance float,RatecodeID int,store_and_fwd_flag string,PULocationID int ,DOLocationID int,payment_type float,fare_amount float,extra float,mta_tax float,tip_amount float,tolls_amount float,improvement_surcharge float,total_amount float,congestion_surcharge float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/csv_data/yellow_tripdata';
