package spark.Sparks3

import org.apache.spark.sql.SparkSession

object Lesson_a4_JOB_Stages_Task {
  def lesson4()
  {
    
        val spark=SparkSession.builder()
        .appName(name="Job Stages and Tasks")
        .master(master="local[3]")
       .config("spark.sql.shuffle.partitions",2)
        .getOrCreate()
    val properties=Map(("header"->"true"),"inferSchema"->"true")
   
   val df1=spark.read
   .options(properties)
   . option("header","true")
   .csv("D:\\Bigdata\\Sample.csv")//Return Single Partition 
  
    val df=df1.repartition(numPartitions=2)//So we get 2 partitions of data file to stimulate real distributed Environment 
   .where("Age<40")//Dataset[Row]
   .select("Age","Gender","Country","no_employees")//DataFrame  
   .groupBy("Country")//RelationGroupedDataset
   .count()//Dataframe
  
   println(df.collect().mkString("-->"))// Collect action Returns Dataframe as Scala Array
  
   scala.io.StdIn.readLine()//So program do not end and we may check UI --> should only be used for testing purpose
  }
}