CREATE DATABASE nyc;

CREATE EXTERNAL TABLE nyc.raw_green(
  `vendorid` bigint,
  `lpep_pickup_datetime` string,
  `lpep_dropoff_datetime` string,
  `store_and_fwd_flag` string,
  `ratecodeid` bigint,
  `pulocationid` bigint,
  `dolocationid` bigint,
  `passenger_count` bigint,
  `trip_distance` double,
  `fare_amount` double,
  `extra` double,
  `mta_tax` double,
  `tip_amount` double,
  `tolls_amount` double,
  `ehail_fee` string,
  `improvement_surcharge` double,
  `total_amount` double,
  `payment_type` bigint,
  `trip_type` bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://nanhyama-nyc/raw/green/'
TBLPROPERTIES (
  'classification'='csv',
  'columnsOrdered'='true',
  'compressionType'='none',
  'delimiter'=',',
  'skip.header.line.count'='1',
  'typeOfData'='file');
