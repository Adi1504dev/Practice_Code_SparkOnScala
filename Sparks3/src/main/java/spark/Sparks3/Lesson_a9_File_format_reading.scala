package spark.Sparks3

import org.apache.spark.sql.SparkSession

object Lesson_a9_File_format_reading {
  def lesson9()
  {
       val spark = SparkSession.builder()
      .appName("File Formats")
      .master("local[3]")
      .getOrCreate()
      
      val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "D:\\Bigdata\\flight*.csv")
      //.option("inferSchema", "true")//After uncommenting the schema is infered 
      .load()


    flightTimeCsvDF.show(5)
   println("CSV Schema:" + flightTimeCsvDF.schema.simpleString)
   
   val flightTimeJsonDF = spark.read
      .format("json")
      .option("path", "D:\\Bigdata\\flight*.json")
      .load()

    flightTimeJsonDF.show(5)
    println("JSON Schema:" + flightTimeJsonDF.schema.simpleString)

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "D:\\Bigdata\\flight*.parquet")
      .load()

    flightTimeParquetDF.show(5)
    println("Parquet Schema:" + flightTimeParquetDF.schema.simpleString)

   
  }
}