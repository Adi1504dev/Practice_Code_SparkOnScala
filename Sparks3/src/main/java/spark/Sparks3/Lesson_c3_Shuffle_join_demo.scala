package spark.Sparks3

import org.apache.spark.sql.SparkSession

object Lesson_c3_Shuffle_join {
    def Lesson3(){

    val spark = SparkSession.builder()
      .appName("Join Demo")
      .master("local[3]")
      .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    val flightTimeDF1 = spark.read.json("D:\\Bigdata\\input_data\\d1")
    val flightTimeDF2 = spark.read.json("D:\\Bigdata\\input_data\\d2")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    val joinExpr = flightTimeDF1.col("id") === flightTimeDF2.col("id")

    val joinDF = flightTimeDF1.join(flightTimeDF2, joinExpr, "inner")

    joinDF.foreach(_ => ())
    scala.io.StdIn.readLine()
  }
}