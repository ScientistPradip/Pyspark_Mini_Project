from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("Car Power Analysis").getOrCreate()

    data = [
        ("Ford Torino", 140, 3449, "US"),
        ("Chevrolet Monte Carlo", 150, 3761, "US"),
        ("BMW 2002", 113, 2234, "Europe")
    ]

    df = spark.createDataFrame(
        data,
        ["carr", "horsepower", "weight", "origin"]
    )

    df.show()

    df2 = df.withColumnRenamed("carr", "car")
    df2.show()