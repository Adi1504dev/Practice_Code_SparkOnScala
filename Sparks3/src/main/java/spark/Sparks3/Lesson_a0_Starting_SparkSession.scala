package spark.Sparks3

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.util.SignalLogger.Handler

import java.util.logging.Handler


object Lesson_a0_Starting_SparkSession {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def lesson0()
  {


    
 
    logger.info("Starting first log application")
   
    val spark=SparkSession.
    builder().
    appName(name="Creating Spark Object").
    master(master="local[3]").getOrCreate()

    logger.info("End of first Spark application")    
    
  }
}