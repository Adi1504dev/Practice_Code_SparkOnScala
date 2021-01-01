package spark.Sparks3

import org.apache.spark.sql.SparkSession

object Lesson_a2_Reading_Data_csv {
  
  def lesson2()
  {
       val spark=SparkSession.builder().appName(name="Reading CSV File").master(master="local[3]").getOrCreate()
    val properties=Map(("header"->"true"),"inferSchema"->"true")
   
   val df=spark.read.options(properties). option("header","true").csv("D:\\Bigdata\\Sample_csv.csv")//options-->take property file/ or properties
   //option takes single property like option("header","true")
   df.printSchema()
    df.show()
  }
}