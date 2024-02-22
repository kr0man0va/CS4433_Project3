import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaskA3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("TaskA3").getOrCreate()

    val t1DF = spark.read.option("header", true).csv("src/main/data/output/T1.csv")
    val customersDF = spark.read.option("header", true).csv("src/main/data/transactions/customers.csv").filter("Age >= 18 AND Age <= 25")

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