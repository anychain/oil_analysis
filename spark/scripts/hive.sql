create table default.north_dakota_montana_production (
  API_NO string,
  POOL string,
  `DATE` string,
  OILBBL string,
  WATERBBL string,
  GASMCF string,
  DAYSPRODUCED string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' LINES TERMINATED BY '\n';
LOAD DATA INPATH 'hdfs://arahant-01:9000/oil/North_Dakota_Montana_production.csv' into table default.north_dakota_montana_production;

create table default.north_dakota_montana_index (
  API_NO string,
  CURRENTOPERATOR string,
  CURRENTWELLNAME string,
  LEASENAME string,
  LEASENUMBER string,
  APPROVEDATE string,
  SPUDDATE string,
  COMPLETIONDATE string,
  TD string,
  COUNTYNAME string,
  FIELDNAME string,
  PRODUCEDPOOLS string,
  OILWATERGASCUMS string,
  IPTDATEOILWATERGAS string,
  SLANT string,
  LATITUDE string,
  LONGITUDE string,
  WELLTYPE string,
  WELLSTATUS string,
  WELLSTATUSDATE string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' LINES TERMINATED BY '\n';
LOAD DATA INPATH 'hdfs://arahant-01:9000/oil/North_Dakota_Montana_index.csv' into table default.north_dakota_montana_index;

create table default.texas_index (
  OIL_GAS_CODE string,
  DISTRICT_NO string,
  LEASE_NO string,
  CYCLE_YEAR string,
  CYCLE_MONTH string,
  CYCLE_YEAR_MONTH string,
  LEASE_NO_DISTRICT_NO string,
  OPERATOR_NO string,
  FIELD_NO string,
  FIELD_TYPE string,
  GAS_WELL_NO string,
  PROD_REPORT_FILED_FLAG string,
  LEASE_OIL_PROD_VOL string,
  LEASE_OIL_ALLOW string,
  LEASE_OIL_ENDING_BAL string,
  LEASE_GAS_PROD_VOL string,
  LEASE_GAS_ALLOW string,
  LEASE_GAS_LIFT_INJ_VOL string,
  LEASE_COND_PROD_VOL string,
  LEASE_COND_LIMIT string,
  LEASE_COND_ENDING_BAL string,
  LEASE_CSGD_PROD_VOL string,
  LEASE_CSGD_LIMIT string,
  LEASE_CSGD_GAS_LIFT string,
  LEASE_OIL_TOT_DISP string,
  LEASE_GAS_TOT_DISP string,
  LEASE_COND_TOT_DISP string,
  LEASE_CSGD_TOT_DISP string,
  DISTRICT_NAME string,
  LEASE_NAME string,
  OPERATOR_NAME string,
  FIELD_NAME string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' LINES TERMINATED BY '\n';
LOAD DATA INPATH 'hdfs://arahant-01:9000/oil/Texas_index.csv' into table default.texas_index;
