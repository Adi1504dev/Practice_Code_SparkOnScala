package spark.Sparks3

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode
 import org.apache.spark.sql.functions.spark_partition_id
object Lesson_b1_Dataframewriter {
    def lesson1()
  {
       val spark = SparkSession.builder()
      .appName("Dataframe Writer")
      .master("local[3]")
      .getOrCreate()
      
   

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "D:\\Bigdata\\flight*.parquet")
      .load()

      //counting number of partitions
      System.out.println(flightTimeParquetDF.rdd.getNumPartitions)
    
      flightTimeParquetDF.show(5)
    println("Parquet Schema:" + flightTimeParquetDF.schema.simpleString)
   
    
    //counting records in each partition
  System.out.println( flightTimeParquetDF.groupBy(spark_partition_id).count().show())
  //partition id has 0 has all records although we have 2 partition no records in second partition  --->hence only 1 output file
  
  //Repartitioning to see if we get more out files
  val RepartionedDF=flightTimeParquetDF.repartition(5)
   System.out.println( RepartionedDF.groupBy(spark_partition_id).count().show())
   
   
  //Writing to the dataframe
    flightTimeParquetDF.write.format("avro").mode(SaveMode.Overwrite).option("path", "D:\\Bigdata\\spark3_output\\").save()
    /*Three Files in the output directory
     * ._SUCCESS.crc---> File indicated the operation was successfull
     * .avro file is the datafile
     * we have .crc file which holds datafile checksum
     * */
    
    //We have 2 partitions but one file----> We counted number of partitions but did not count number of records in each partition 
    //in one partition there are no records
    
    
     RepartionedDF.write.format("avro").mode(SaveMode.Overwrite).option("path", "D:\\Bigdata\\spark3_output1\\").save()
     //five avro file generated
     
     
     //Using partition by feature
   flightTimeParquetDF.write.format("json").mode(SaveMode.Overwrite)
   .partitionBy("OP_CARRIER","ORIGIN")
   .option("path", "D:\\Bigdata\\spark3_output2\\")
   .option("maxRecordsPerFile", 10000)
   .save()  
    System.out.println("Output written in file")
    
  }
}