import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object TaskA1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("TaskA1")getOrCreate()

    // Define the schema for CSV file
    val schema = StructType(
      Array(
        StructField("TransID", IntegerType, true),
        StructField("CustID", IntegerType, true),
        StructField("TransTotal", FloatType, true),
        StructField("TransNumItems", IntegerType, true),
        StructField("TransDesc", StringType, true)
      )
    )

    val df = spark.read.option("header",true).schema(schema).csv("src/main/data/transactions/purchases.csv")

    df.createOrReplaceTempView("purchasesT")

    val t1 = spark.sql("SELECT * FROM purchasesT WHERE TransTotal > 600")

    t1.coalesce(1).write.option("header",true).csv("src/main/data/output/T1.csv")

    t1.show()

    spark.stop()
  }
}
