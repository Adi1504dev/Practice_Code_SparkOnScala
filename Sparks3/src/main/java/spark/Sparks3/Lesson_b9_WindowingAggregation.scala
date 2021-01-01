package spark.Sparks3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Lesson_b9_WindowingAggregation {
  def lesson9()
  {   
    val spark = SparkSession.builder()
      .appName("Window Aggregation")
      .master("local[3]")
      .getOrCreate()
    
      val DF=spark.read.parquet(("D:\\Bigdata\\aggregation\\*.parquet"))
      DF//.show
      val RunningTotalWindow_all_previous_week=Window.partitionBy("Country")
      .orderBy("WeekNumber")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
      
         val RunningTotalWindow_past2_week=Window.partitionBy("Country")
      .orderBy("WeekNumber")
      .rowsBetween(-2, Window.currentRow-1)
      
      val summaryDF=DF.withColumn("RunningTotal", sum("InvoiceValue").over(RunningTotalWindow_all_previous_week))
      
      val summaryDF2=DF.withColumn("RunningTotal", sum("InvoiceValue").over(RunningTotalWindow_past2_week))
 
      println("Running Total for all previous weeks")
 summaryDF.show
   println("Running Total for last 2 weeks")
 summaryDF2.show
 
  }
}