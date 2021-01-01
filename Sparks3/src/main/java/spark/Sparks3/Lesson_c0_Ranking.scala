package spark.Sparks3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Lesson_c0_Ranking {
  def Lesson0 {

    val spark = SparkSession.builder()
      .appName("Ranking Demo")
      .master("local[3]")
      .getOrCreate()

    val summaryDF = spark.read.parquet("D:\\Bigdata\\input_data\\summary.parquet")

    summaryDF.sort("Country", "WeekNumber").show()

    val rankWindow = Window.partitionBy("Country")
      .orderBy(col("InvoiceValue").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summaryDF.withColumn("Rank", dense_rank().over(rankWindow))
      .where(col("Rank") ===1)
      .sort("Country", "WeekNumber")
      .show()

    spark.stop()
  }
}