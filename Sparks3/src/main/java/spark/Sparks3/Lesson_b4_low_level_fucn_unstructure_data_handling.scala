package spark.Sparks3

import org.apache.spark.sql.SparkSession
case class ApacheLogRecord(ip: String, date: String, request: String, referrer: String)
object Lesson_b4_low_level_fucn_unstructure_data_handling {
  def lesson4()
  {
    val spark=SparkSession.builder()
    .appName("low level function and unstructured data")
    .master("local[3]").enableHiveSupport().getOrCreate()
    
    var unstuct_data=spark.read.textFile("D:\\Bigdata\\apache_logs.txt")//Retuns Dataset[String]
    .toDF()
    val myReg = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r
    
    import spark.implicits._
    val unstuct_data1=unstuct_data.map(row=>//remember the variable should be of type val 
      row.getString(0) match{
        case myReg(ip, client, user, date, cmd, request, proto, status, bytes, referrer, userAgent) => 
       ApacheLogRecord(ip, date, request, referrer)
      }
    )
      
  import org.apache.spark.sql.functions._
 unstuct_data1. where("trim(referrer) != '-' ")
      .withColumn("referrer", substring_index($"referrer", "/", 3))
     .groupBy("referrer").count()//count would be require with group by and will return grouped column and join 
    .show(truncate = false)
    spark.stop()
  }

}