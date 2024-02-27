import org.apache.spark.sql.SparkSession

object TaskA1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("TaskA1")getOrCreate()

    val df = spark.read.option("header",true).csv("src/main/data/transactions/purchases.csv")

    df.createOrReplaceTempView("purchasesT")

    val t1 = spark.sql("SELECT * FROM purchasesT WHERE TransTotal > 600")

    t1.coalesce(1).write.option("header",true).csv("src/main/data/output/T1.csv")

    t1.show()

    spark.stop()
  }
}
