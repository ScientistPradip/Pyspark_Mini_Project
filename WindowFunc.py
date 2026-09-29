from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import lag
from pyspark.sql.functions import lead


if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    simpleData = (("James", "Sales", 3000), \
 \
                  ("Michael", "Sales", 4600), \
 \
                  ("Robert", "Sales", 4100), \
 \
                  ("Maria", "Finance", 3000), \
 \
                  ("James", "Sales", 3000), \
 \
                  ("Scott", "Finance", 3300), \
 \
                  ("Jen", "Finance", 3900), \
 \
                  ("Jeff", "Marketing", 3000), \
 \
                  ("Kumar", "Marketing", 2000), \
 \
                  ("Saif", "Sales", 4100))

    columns = ["employee_name", "department", "salary"]

    df = spark.createDataFrame(data=simpleData, schema=columns)

    df.printSchema()

    df.show(truncate=False)

    windowSpec = Window.partitionBy("department").orderBy("salary")
    df.withColumn("row_number",row_number().over(windowSpec)).show(truncate=False)

    df.withColumn("rank", rank().over(windowSpec)).show()

    df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

    df.withColumn("lag", lag("salary", 2).over(windowSpec)).show()

    df.withColumn("lead", lead("salary", 2).over(windowSpec)).show()

