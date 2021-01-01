package spark.Sparks3

import org.apache.spark.sql.SparkSession

object Lesson_a3_Transformations_in_Dataframe {
  def lesson3()
  {
        val spark=SparkSession.builder().appName(name="Transforming DataFrame").master(master="local[3]").getOrCreate()
    val properties=Map(("header"->"true"),"inferSchema"->"true")
   
   val df=spark.read.options(properties). option("header","true").csv("D:\\Bigdata\\Sample_csv.csv")
  df.where("construction='Wood'").show()
}

}