package spark.Sparks3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._

object Lesson_b5_Column_Transformation {
  def lesson5()
  {
    val spark=SparkSession.builder()
    .appName("Column Transformation")
    .master("local[3]").enableHiveSupport().getOrCreate()
     
    val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "D:\\Bigdata\\flight_data.cvs")//Name does not matter i format csv is define even if file is cvs
      .load()
      
      //Using Column String
      flightTimeCsvDF.select("Month", "Year","CRSDepTime","DayofMonth")//.show()
      
      //Using Column Object
      flightTimeCsvDF.select(column("Origin"), col("Dest"), col("Distance"), col("IsDepDelayed") )//.show(10) 
     
      //String Expression 
      //Way 1
      
      
      flightTimeCsvDF.select(col("Origin"), column("Dest"),expr("Distance"), expr("to_date(concat(Year,Month,DayofMonth),'yyyyMMdd') as FlightDate"))//.show(10)
  //Way 2
  flightTimeCsvDF.selectExpr("Origin", "Dest", "Distance", "to_date(concat(Year,Month,DayofMonth),'yyyyMMdd') as FlightDate").show(10)
 
      //Column Object Expression
      
      flightTimeCsvDF.select(col("Origin"), col("Dest"),col("Distance"), to_date(concat(col("Year"),col("Month"),col("DayofMonth")),"yyyyMMdd").as("FlightDate")).show(10)
 
      
  }
}