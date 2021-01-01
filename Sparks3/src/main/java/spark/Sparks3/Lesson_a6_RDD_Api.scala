package spark.Sparks3
import java.io.Serializable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import breeze.macros.expand.args
import org.apache.log4j.Logger
import breeze.macros.expand.args
 //case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)
object Lesson_a6_RDD_Api extends Serializable {

  def lesson6()
  {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  

    //Create Spark Context
    val sparkAppConf = new SparkConf().setAppName("HelloRDD").setMaster("local[3]")
    val sparkContext = new SparkContext(sparkAppConf)

    //Read your CSV file
    val linesRDD = sparkContext.textFile("D:\\Bigdata\\Sample.csv",2)

    //Give it a Structure and select only 4 columns
   
    val colsRDD = linesRDD.map(line => {
      val cols = line.split(",").map(_.trim)
      SurveyRecord(cols(1).toInt, cols(2), cols(3), cols(4))
    })

    //Apply Filter
    val filteredRDD = colsRDD.filter(r => r.Age < 40)

    //Manually implement the GroupBy
    val kvRDD = filteredRDD.map(r => (r.Country, 1))
    val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)

    //Collect the result
    println(countRDD.collect().mkString(","))

    //Stop the Spark context
    //scala.io.StdIn.readLine()
    sparkContext.stop()
  }

}
