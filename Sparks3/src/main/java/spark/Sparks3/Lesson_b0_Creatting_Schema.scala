package spark.Sparks3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructField

object Lesson_b0_Creatting_Schema {
  def lesson0()
  {
       val spark = SparkSession.builder()
      .appName("Schema Creation")
      .master("local[3]")
      .getOrCreate()
      
      //Using programatic approcah to create Schema
          val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))

//Using DDL approach to create Schema
    val flightSchemaDDL = "FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, " +
      "ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, " +
      "WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"

     
      val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")//Tell what to do on failure
      .option("path", "D:\\Bigdata\\flight*.csv")
      .schema(flightSchemaStruct)
  .option("dateFormat", "M/d/y")//Provide to define the format of date String
      //.option("inferSchema", "true")//After uncommenting the schema is infered 
      .load()


    flightTimeCsvDF.show(5)
   println("CSV Schema:" + flightTimeCsvDF.schema.simpleString)
   
   val flightTimeJsonDF = spark.read
      .format("json")
      .option("mode", "FAILFAST")
      .option("path", "D:\\Bigdata\\flight*.json")
      .schema(flightSchemaDDL)
  .option("dateFormat", "M/d/y")
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