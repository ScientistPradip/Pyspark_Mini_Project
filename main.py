from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    data = [1,2,3,4,5,6,7,8,9,10,11,12]
    rdd=spark.sparkContext.parallelize(data)
    print(rdd.count())



