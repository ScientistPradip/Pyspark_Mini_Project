from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Creating Spark Session
    data = [["1", "2020-02-01"], ["2", "2019-03-01"], ["3", "2021-03-01"]]
    df=spark.createDataFrame(data,["id","input"])
    df.show()

    #Current Data() Func
    df.select(current_date().alias("current_date")).show(1)

    #The below example uses "date_format()" to parses the date and converts from yyyy-dd-mm to MM-dd-yyyy format.
    df.select(col("input"),date_format(col("input"), "MM-dd-yyyy").alias("date_format")).show()

    #to_date()
    #Below example converts string in date format yyyy-MM-dd to a DateType yyyy-MM-dd using to_date(). You can also use this to convert into any specific format. PySpark supports all patterns supports on Java
    df.select(col("input"),to_date(col("input"), "yyy-MM-dd").alias("to_date")).show()

    #datediff()
    #The below example returns the difference between two dates using datediff().
    df.select(col("input"), datediff(current_date(), col("input")).alias("datediff")).show()


    #months_between()
    #The below example returns the months between two dates using months_between().
    df.select(col("input"), months_between(current_date(), col("input")).alias("months_between")).show()

    #add_months() , date_add(), date_sub()
    #Here we are adding and subtracting date and month from a given input.
    df.select(col("input"),
        add_months(col("input"),3).alias("add_months"),
        add_months(col("input"), -3).alias("sub_months"),
        date_add(col("input"),4).alias("date_add"),
        date_sub(col("input"), 4).alias("date_sub")
        ).show()

    #year(), month(), month(),next_day(), weekofyear()
    df.select(col("input"),
        year(col("input")).alias("year"),
        month(col("input")).alias("month"),
        next_day(col("input"), "Sunday").alias("next_day"),
        weekofyear(col("input")).alias("weekofyear")
        ).show()

    #dayofweek(), dayofmonth(), dayofyear()
    df.select(col("input"),
    dayofweek(current_date()).alias("dayofweek"),
    dayofmonth(current_date()).alias("dayofmonth"),
    dayofyear(current_date()).alias("dayofyear"),
    ).show()


    #dayofweek(), dayofmonth(), dayofyear()
    df.select(col("input"),
        dayofweek(current_date()).alias("dayofweek"),
        dayofmonth(current_date()).alias("dayofmonth"),
        dayofyear(current_date()).alias("dayofyear"),
        ).show()


    #current_timestamp()
    #Following are the Timestamp Functions that you can use on SQL and on DataFrame.
    data = [["1", "02-01-2020 11 01 19 06"], ["2", "03-01-2019 12 01 19 406"], ["3", "03-01-2021 12 01 19 406"]]
    df2 = spark.createDataFrame(data, ["id", "input"])
    df2.show(truncate=False)

    #Below example returns the current timestamp in spark default format yyyy-MM-dd HH:mm:ss
    df2.select(current_timestamp().alias("current_timestamp")).show(1, truncate=False)

    #to_timestamp()
    #Converts string timestamp to Timestamp type format.
    df2.select(col("input"), to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp")).show(
        truncate=False)

    #hour(), Minute() and second()
    data = [["1", "2020-02-01 11:01:19.06"], ["2", "2019-03-01 12:01:19.406"], ["3", "2021-03-01 12:01:19.406"]]
    df3 = spark.createDataFrame(data, ["id", "input"])
    df3.select(col("input"),
       hour(col("input")).alias("hour"),
       minute(col("input")).alias("minute"),
       second(col("input")).alias("second")
    ).show(truncate=False)