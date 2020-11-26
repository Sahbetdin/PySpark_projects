'''
26/11/2020
This PySpark program uses france.csv file which contains all addresses in France.
File has these columns:

['LON', 'LAT', 'NUMBER', 'STREET', 'CITY', 'POSTCODE', 'ID', 'HASH']
We drop columns 'UNIT', 'DISTRICT', 'REGION' because they consist of NULL.
Then we extract all cities and put them into 'all_c' list.
len(all_c) = 33115.
We make two loop where we select rows for i-th and j-th cities and crossjoin them.
Before joining we 
Then we add new column which is just concatenation of HASH

SOURCE:
https://www.kaggle.com/openaddresses/openaddresses-europe?select=france.csv
'''

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
import time

def create_spark():
	conf = pyspark.SparkConf().setMaster("local[*]")
	spark = SparkSession \
    	.builder \
    .appName("France_addresses") \
    .config(conf=conf) \
    .getOrCreate()
	return spark

# spark = SparkSession.builder.appName("France_addresses").config(conf=conf).getOrCreate()

def read_data(spark, str):
	df = spark.read.csv(str, inferSchema=True, header=True)
	df = df.drop('UNIT').drop('DISTRICT').drop('REGION')
	return df

def max_min_lat_lon(df):
	lon = df.select('lon')
	lon_min = lon.agg(F.min('lon'))
	lon_min = lon_min.collect()
	lon_min = lon_min[0]['min(lon)']

	lon_max = lon.agg(F.max('lon'))
	lon_max = lon_max.collect()
	lon_max = lon_max[0]['max(lon)']

	lat = df.select('lat')
	lat_min = lat.agg(F.min('lat'))
	lat_min = lat_min.collect()
	lat_min = lat_min[0]['min(lat)']

	lat_max = lat.agg(F.max('lat'))
	lat_max = lat_max.collect()
	lat_max = lat_max[0]['max(lat)']
	return (lon_min, lon_max, lat_min, lat_max)

def get_all_cities(df):
	city = df.select('city')
	ci = city.groupBy('city').count()
	all_cities = ci.collect()
	all_c = list(zip(*all_cities))[0]
	return all_c

def process_joins(n, df, all_c):
	for i in range(n):
		for j in range(i + 1, n):
			df1 = df.where(
			"city='{}'".format(all_c[i])
			)
			df2 = df.where(
				"city='{}'".format(all_c[j])
			)
			print(df1.count(), df2.count())
			df1.show(3, False)
			df2.show(3, False)
			df1 = df1.selectExpr("LON as LON1", "LAT as LAT1", "NUMBER as NUMBER1", "STREET as STREET1", "CITY as CITY1",
						"POSTCODE as POSTCODE1", "ID as ID1", "HASH as HASH1")
			df2 = df2.selectExpr("LON as LON2", "LAT as LAT2", "NUMBER as NUMBER2", "STREET as STREET2", "CITY as CITY2",
						"POSTCODE as POSTCODE2", "ID as ID2", "HASH as HASH2")
			df_total = df1.crossJoin(df2)
			df_total = df_total.withColumn('HASHES', F.concat(F.col('HASH1'), F.col('HASH2')))
			df_total.show(5, False)    


def main():
	start = time.time()
	spark = create_spark()
	sdf = read_data(spark, 'france.csv')
	sdf.show(10, False)
	print(sdf.columns)
	all_c = get_all_cities(sdf)
	n = 33000 #total number of cites if len(all_c) = 33115. We didn't take all of them
	process_joins(n, sdf, all_c)
	print("time elapsed: ", time.time() - start)


if __name__ == "__main__":
    main()
