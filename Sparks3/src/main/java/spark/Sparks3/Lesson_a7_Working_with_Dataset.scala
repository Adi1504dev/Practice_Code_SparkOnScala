package spark.Sparks3

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object Lesson_a7_Working_with_Dataset extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def Lesson7() {

  
    //Create Spark Session
    val spark = SparkSession.builder()
      .appName("Hello DataSet")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._


    //Read your CSV file
    val rawDF:Dataset[Row] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\Bigdata\\Sample.csv")

    //Type Safe Data Set
    val surveyDS:Dataset[SurveyRecord] = rawDF.select("Age", "Gender", "Country", "state").as[SurveyRecord]

    //Type safe Filter
    val filteredDS = surveyDS.filter(r => r.Age < 40)//you can not use age here instead of Age-->TypeSafe
    //Runtime Filter
    val filteredDF = surveyDS.filter("Age  < 40")//you can\ use age here instead of Age--> not TypeSafe--> will throw runtime error

    //Type safe GroupBy
    val countDS = filteredDS.groupByKey(r => r.Country).count()
    //Runtime GroupBy
    val countDF = filteredDF.groupBy("Country").count()

    println("DataFrame: " + countDF.collect().mkString(","))
    println("DataSet: " + countDS.collect().mkString(","))

    //Uncomment if you want to investigate SparkUI
    //scala.io.StdIn.readLine()
    spark.stop()
  }

}