package spark.Sparks3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Lesson_c4_bucket_join {
    def Lesson4(){
//Remember using hive u need to delete metastore_db   
       val spark = SparkSession.builder()
      .appName("Bucket join")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()
      

        val df1 = spark.read.json("D:\\Bigdata\\input_data\\d1")
        val df2 = spark.read.json("D:\\Bigdata\\input_data\\d2")
        //df1.show()
        //df2.show()

           //Creating Spark Table
      spark.sql("Create database IF NOT EXISTS My_DB")
      
    //Changing current database can also use db.table_name in saveAsTable()
       spark.catalog.setCurrentDatabase("MY_DB")
      
      spark.catalog.listTables("MY_DB").show()
      
        df1.coalesce(1).write
          .bucketBy(3, "id")
          .mode(SaveMode.Overwrite)
          .saveAsTable("MY_DB.flight_data1")

        df2.coalesce(1).write
          .bucketBy(3, "id")
          .mode(SaveMode.Overwrite)
          .saveAsTable("MY_DB.flight_data2")


    val df3 = spark.read.table("MY_DB.flight_data1")
    val df4 = spark.read.table("MY_DB.flight_data2")

    val joinExpr = df3.col("id") === df4.col("id")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val joinDF = df3.join(df4, joinExpr, "inner")

    joinDF.foreach(_ => ())
    scala.io.StdIn.readLine()


  }
}