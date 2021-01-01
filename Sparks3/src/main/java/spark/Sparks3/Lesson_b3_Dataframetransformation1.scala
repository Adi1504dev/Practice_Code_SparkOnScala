package spark.Sparks3

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Lesson_b3_Dataframetransformation1 {
  def toDateDF(df:DataFrame, fmt:String, fld:String):DataFrame = {
  df.withColumn(fld, to_date(col("EventDate"),fmt))  
} 
  def lesson3()
  {val spark = SparkSession.builder()
      .appName("Transform Dataset 1")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()
      
    //Creating Schema
    val schema=StructType(List(StructField("ID",StringType),StructField("EventDate",StringType)))
    
    //creating Dataframe rows
    val data=List(Row("1","02/08/2020"),Row("2","07/08/2020"),Row("3","03/08/2020"),Row("4","04/08/2020"))
val myRDD = spark.sparkContext.parallelize(data, 2)
    val myDF = spark.createDataFrame(myRDD,schema)
    myDF.printSchema
myDF.show
val newDF = toDateDF(myDF,  "M/d/y", "EventDate1")
newDF.printSchema
newDF.show  
    
   
    
  }
}