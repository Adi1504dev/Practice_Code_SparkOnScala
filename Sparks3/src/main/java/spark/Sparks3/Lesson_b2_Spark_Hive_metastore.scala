package spark.Sparks3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Lesson_b2_Spark_Hive_metastore {
  def lession2()
  {
    val spark = SparkSession.builder()
      .appName("Spark Hive")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()
      
//Remember using hive u need to delete metastore_db   

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "D:\\Bigdata\\input_data\\flight*.parquet")
      .load()
      
      //Creating Spark Table
      spark.sql("Create database IF NOT EXISTS Airline_db")
      
    //Changing current database can also use db.table_name in saveAsTable()
       spark.catalog.setCurrentDatabase("Airline_db")
      
      spark.catalog.listTables("Airline_db").show()
      
       flightTimeParquetDF.write.format("json")
       .mode(SaveMode.Overwrite)
      // .partitionBy("ORIGIN","OP_CARRIER")
       .bucketBy(6,"ORIGIN","OP_CARRIER").sortBy("ORIGIN","OP_CARRIER")
       //Table will be external as we are providing path--> still we can see changes
       .option("path", "D:\\Spark3\\spark3_output2\\")// Setting up path as my default path was acting wierdly
       .saveAsTable("Filght_data")
       
       
  }

}