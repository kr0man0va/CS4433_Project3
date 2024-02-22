import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaskA2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("TaskA2")getOrCreate()

    val df = spark.read.option("header",true).csv("src/main/data/output/T1.csv")

    //Calculate approx median => wrong answer when even number
//    val t2 = df.groupBy("TransNumItems")
//      .agg(
//        expr("percentile_approx(TransTotal, 0.5)").alias("MedianTotal"),
//        min("TransTotal").alias("MinTotal"),
//        max("TransTotal").alias("MaxTotal")
//      )

    val t2 = df.groupBy("TransNumItems")
      .agg(
        min("TransTotal").alias("minTotal"),
        max("TransTotal").alias("maxTotal")
      )

    // Calculate the exact median
    val median = df.groupBy("TransNumItems")
      .agg(
        expr("sort_array(collect_list(TransTotal))").alias("sortedTransTotal"),
        size(collect_list("TransTotal")).alias("count")
      )
      .withColumn("median", when(col("count") % 2 === 0,
        (col("sortedTransTotal")((col("count").cast("int") / 2 - 1).cast("int")) +
          col("sortedTransTotal")((col("count").cast("int") / 2).cast("int"))) / 2.0
      ).otherwise(
        col("sortedTransTotal")((col("count") / 2).cast("int"))
      ))
      .drop("sortedTransTotal", "count")

    // Combine t2 and median using a join
    val combinedResult = t2.join(median, "TransNumItems")

    combinedResult.coalesce(1).write.option("header",true).csv("src/main/data/output/T2.csv")

//    combinedResult.show()

    spark.stop()
  }
}
