from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# spark is from the previous example
sc = spark.sparkContext

customers_schema = """
    ID INT,
    Name STRING,
    Age INT,
    CountryCode INT,
    Salary FLOAT
"""

purchases_schema = """
    TransID INT,
    CustID INT,
    TransTotal FLOAT,
    TransNumItems INT,
    TransDesc STRING
"""


# A CSV dataset is pointed to by path.
# The path can be either a single CSV file or a directory of CSV files
purchpath = "C:/Users/keabo/Documents/GitHub/CS4433_Project3/src/main/data/transactions/purchases.csv"
custpath = "C:/Users/keabo/Documents/GitHub/CS4433_Project3/src/main/data/transactions/customers.csv"
purchdf = spark.read.csv(purchpath, header=False, schema=purchases_schema)
custdf = spark.read.csv(custpath, header=False, schema=customers_schema)

# Task 2.A.1
T1_df = purchdf.filter(purchdf['TransTotal'] > 600)

T1_df.createOrReplaceTempView("T1")

T1_df.show(n=100)

# Task 2.A.2
result_df = spark.sql("""
SELECT
    TransNumItems,
    percentile_approx(TransTotal, 0.5) AS MedianTransTotal,
    MIN(TransTotal) AS MinTransTotal,
    MAX(TransTotal) AS MaxTransTotal
FROM
    T1
GROUP BY
    TransNumItems;
""")

# Show the result
result_df.show(n=100)

# Task 2.A.3
young_customers_df = custdf.filter((custdf['Age'] >= 18) & (custdf['Age'] <= 25))
young_customers_df.createOrReplaceTempView("young_customers")

T3_df = spark.sql("""
    SELECT T1.CustID,
           young_customers.Age,
           SUM(T1.TransNumItems) AS total_num_items,
           SUM(T1.TransTotal) AS total_amount_spent
    FROM T1
    INNER JOIN young_customers
    ON T1.CustID = young_customers.ID
    GROUP BY T1.CustID, young_customers.Age
""")

T3_df.createOrReplaceTempView("T3")

T3_df.show()

# Stop the Spark session
spark.stop()