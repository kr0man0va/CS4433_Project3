import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TaskA3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("TaskA3").getOrCreate()

    // Define the schema for CSV file
    val schema1 = StructType(
      Array(
        StructField("TransID", IntegerType, true),
        StructField("CustID", IntegerType, true),
        StructField("TransTotal", FloatType, true),
        StructField("TransNumItems", IntegerType, true),
        StructField("TransDesc", StringType, true)
      )
    )

    val schema2 = StructType(
      Array(
        StructField("ID", IntegerType, true),
        StructField("Name", StringType, true),
        StructField("Age", IntegerType, true),
        StructField("CountryCode", IntegerType, true),
        StructField("Salary", FloatType, true)
      )
    )

    val t1DF = spark.read.option("header", true).schema(schema1).csv("src/main/data/output/T1.csv")
    val customersDF = spark.read.option("header", true).schema(schema2).csv("src/main/data/transactions/customers.csv").filter("Age >= 18 AND Age <= 25")

    val joinedDF = t1DF.join(customersDF, t1DF("CustID") === customersDF("ID"))

    val grouped = joinedDF.groupBy("CustID", "Age")

    val t3 = grouped.agg(
      sum("TransNumItems").alias("TotalNumItems"),
      sum("TransTotal").alias("TotalAmountSpent")
    )

    t3.coalesce(1).write.option("header", true).csv("src/main/data/output/T3.csv")

    t3.show()

    spark.stop()
  }
}