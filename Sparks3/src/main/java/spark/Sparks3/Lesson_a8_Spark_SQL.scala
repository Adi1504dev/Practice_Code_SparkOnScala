package spark.Sparks3

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object Lesson_a8_Spark_SQL {
  def Lesson8() {

  
    //Create Spark Session
    val spark = SparkSession.builder()
      .appName("Spark Sql")
      .master("local[3]")
      .getOrCreate()
    
      val surveyDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\Bigdata\\Sample.csv")
      
    
    surveyDF.createOrReplaceTempView("survey_tbl")

    val countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")
 countDF.show
  }
}