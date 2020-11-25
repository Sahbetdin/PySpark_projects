import pyspark
import time
import os
import shutil

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext

import pyspark.sql.functions as F
#for user defined funstions
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType, FloatType, ArrayType

#necessary for calculating root of discriminant
from math import sqrt
#for generating random numbers
from random import randint, random

def create_spark():
	conf = pyspark.SparkConf().setMaster("local[*]")
	spark = SparkSession \
    	.builder \
    .appName("Covid and prices") \
    .config(conf=conf) \
    .getOrCreate()
	return spark


#define column types
def define_schema():
	schema = StructType([
    	StructField('a', DoubleType(), False),
    	StructField('b', DoubleType(), False),
    	StructField('c', DoubleType(), False),
    	StructField('d', DoubleType(), False),
    	StructField('x1', DoubleType(), True),
    	StructField('x2', DoubleType(), True)
	])
	return schema

#generate coefficents a,b,c for quadratic equation a*x^2 + b*x + c
def define_coeff(n):
	vals = list()
	for i in range(n):
		a = 20 * random() - 8
		b = 10 * random() + 4
		c = 6 * random()-7
		d = b ** 2 - 4 * a * c
		if d >= 0:
			x1 = (-b - sqrt(d)) / 2 / a
			x2 = (-b + sqrt(d)) / 2 / a
		elif d == 0:
			x1 = x2 = -b / 2 / a
		else:
			x1 = x2 = None
		vals.append((a,b,c,d,x1,x2))
	return vals

#calculate value of quadratic function
def f(x,a,b,c):
    return a * x ** 2 + b * x + c

#find root of quadratice equation in case it exists
#by Binary search
def solve_q_eq(left, right, a, b, c):
    eps = 1.e-8
    if f(right,a,b,c) >= 0:
        while abs(right - left) > eps:
            mid = (left + right) / 2
            tmp = f(mid,a,b,c)
            if tmp > 0:
                right = mid
            elif tmp < 0:
                left = mid
            else:
                break
    else:
        while abs(right - left) > eps:
            mid = (left + right) / 2
            f_left = f(left,a,b,c)
            tmp = f(mid,a,b,c)
            if tmp > 0:
                left = mid
            elif tmp < 0:
                right = mid
            else:
                break
    return (mid)

#use previously defined 'solve_q_eq' function for finding both roots
#of quadratic equation
def solve_q_eq_totally(a,b,c):
    left = -20
    right = 20
    d = b ** 2 - 4 * a * c
    if d >= 0:
        x1 = solve_q_eq(left, right, a, b, c)
        right = x1 - 0.01
        x2 = solve_q_eq(left, right, a, b, c)
    else:
        x1 = None
        x2 = None
    return (x1, x2)

# this schema is for outputs of function 'solve_udf' which is PySpark version
# of 'solve_q_eq_totally'
def define_schema_roots():
	array_schema = StructType([
    	StructField('x1_calc', DoubleType(), nullable=True),
    	StructField('x2_calc', DoubleType(), nullable=True)
    	])
	return array_schema



def main():
	start = time.time()
	try:
		shutil.rmtree("quadr_eq")
		print ("Previous directory 'quadr_eq' as removed")
	except OSError as error:
		print (error)
		print ("Directory is not there, not removed")
	spark = create_spark()
	schema = define_schema()
	n = 3000000000
	vals = define_coeff(n)
	# create DataFrame
	df = spark.createDataFrame(vals, schema)
	roots_schema = define_schema_roots()
	solve_udf = udf(lambda a,b,c: solve_q_eq_totally(a,b,c), roots_schema)
	#save df and found roots column to new df2 DataFrame
	df2 = df.select('a', 'b', 'c','d', 'x1', 'x2', solve_udf('a', 'b', 'c').alias('roots'))
	df2.write.parquet("quadr_eq")
	print("time elapsed: ", time.time() - start)


if __name__ == "__main__":
    main()
