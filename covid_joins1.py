import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
import time

def create_spark():
	conf = pyspark.SparkConf().setMaster("local[*]")
	spark = SparkSession \
    	.builder \
    .appName("Covid and prices") \
    .config(conf=conf) \
    .getOrCreate()
	return spark

def read_data(spark, str):
	df = spark.read.csv(str, inferSchema=True, header=True)
	return df

def get_all_dates(df, col):
	d = df.select(col).distinct().collect()
	d1 = list()
	for x in d:
		d1.append(x[col])
	d1.sort()
	return d1

def process_joins(df):
	d1 = get_all_dates(df,'date')
	n = len(d1)
	s = -1
	flag = True
	for i in range(n):
		if flag==False:
			break
		for j in range(i + 1, n):
			s += 1
			print("s= ", s, "out of ", n ** 2)
			left_table = df.where(
				"date = '{}'".format(d1[i])
			)
			right_table = df.where(
				"date = '{}'".format(d1[j])
			)
			left_table = left_table.withColumnRenamed("date","date1") \
							.withColumnRenamed("cases","cases1") \
							.withColumnRenamed("deaths","deaths1")
			right_table = right_table.withColumnRenamed("date","date2") \
									.withColumnRenamed("cases","cases2") \
									.withColumnRenamed("deaths","deaths2")
			joined = left_table.join(
				right_table,
				on="county",
				how="inner"    
			)
			joined = joined.withColumn("diff_case", F.col("cases2")-F.col("cases1"))
			print(joined.count())
			print("i=" , i, "\tj=", j)
			cases_diff = joined.select("diff_case")
			cases_diff.show()
			if s == 10:
				flag = False
				break

def main():
	start = time.time()
	spark = create_spark()
	sdf = read_data(spark, 'us-counties.csv')
	process_joins(sdf)
	print("time elapsed: ", time.time() - start)


if __name__ == "__main__":
    main()
