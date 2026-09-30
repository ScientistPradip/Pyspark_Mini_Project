from pyspark.sql import SparkSession



#Step1 Loading the Data into DataFrame
if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    data = [
        (1, 101, 5001, "Laptop", "Electronics", 1000.0, 1),
        (2, 102, 5002, "Headphones", "Electronics", 50.0, 2),
        (3, 101, 5003, "Book", "Books", 20.0, 3),
        (4, 103, 5004, "Laptop", "Electronics", 1000.0, 1),
        (5, 102, 5005, "Chair", "Furniture", 150.0, 1)
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "product_id",
        "product_name",
        "category",
        "price",
        "quantity"
    ]

df = spark.createDataFrame(data,columns)
df.show()

#Step2: Using Filter Transformation
df_filtered = df.filter(df.quantity >1)
df_filtered.show()

#Step3: Handling Null Values

average_price = df.selectExpr("avg(price)").collect()[0][0]
df_filled = df.na.fill({"price":average_price})
df_filled.show()

#Step4: Dropping Duplicates
#Drop duplicate rows based on customer_id and product_id
df_no_duplicates = df.dropDuplicates(["customer_id","product_id"])
df_no_duplicates.show()

#Step5: Selecting Specific Columns
df_selected = df.select("customer_id","product_name","price")
df_selected.show()

#Step6: Grouping and Aggregating Data
#Calculated the total spending per customer
df_grouped = df.groupBy("customer_id").agg({'price':'sum'})
df_grouped.show()

#Step7: Joining DataFrames
#Assume we have another DataFrame with customer details
customer_data = [
    (101, "John Doe", "john@example.com"),
    (102, "Jane Smith", "jane@example.com"),
    (103, "Alice Johnson", "alice@example.com")
]
customer_columns = ["customer_id", "customer_name", "email"]
df_customers = spark.createDataFrame(customer_data, customer_columns)

#Join on customer_id
df_joined = df.join(df_customers, on ="customer_id", how = "inner")
df_joined.show()

#Step8: Union of Two DataFrames
# Create another DataFrame with similar schema
new_data = [(6, 104, 5006, 'Table', 'Furniture', 200.0, 1)]
df_new = spark.createDataFrame(new_data,columns)

#Union the DataFrames
df_union = df.union(df_new)
df_union.show()

# Step 9: Creating Temporary Views and Using SQL
df.createOrReplaceTempView("transactions")

#Run SQL query
sql_result = spark.sql("SELECT customer_id, SUM(price * quantity) as total_spent FROM transactions GROUP BY customer_id")
sql_result.show()

# Summary of Transformations and Actions Used:
# - `filter`: Filter rows based on a condition.
# - `na.fill`: Fill null values.
# - `dropDuplicates`: Remove duplicate rows based on specific columns.
# - `select`: Select specific columns.
# - `groupBy` and `agg`: Group data and perform aggregation.
# - `join`: Join two DataFrames on a key.
# - `union`: Combine two DataFrames with the same schema.
# - `createOrReplaceTempView` and `spark.sql`: Create a temporary view and execute SQL queries on the DataFrame.






