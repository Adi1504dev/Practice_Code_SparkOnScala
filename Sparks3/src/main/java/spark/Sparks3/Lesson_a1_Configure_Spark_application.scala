package spark.Sparks3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties
import scala.io.Source


object Lesson_a1_Configure_Spark_application {
  def lesson1()
  {
   // Method 1
    val SparkAppConf=new SparkConf()
    //SparkAppConf.set("spark.app.name", "Configuring Spark Application")
    //SparkAppConf.set("spark.master","local[3]")

// val spark=SparkSession.builder().config(SparkAppConf).getOrCreate()
    
    
   //Method 2
   val spark=SparkSession.builder().config(Sparkconf).getOrCreate()
   
   //To view all spark configurations
   println(spark.conf.getAll.toString)
  } 
  
  
  def Sparkconf: SparkConf=
  {val SparkAppConf=new SparkConf()
     val props=new  Properties
    props.load(Source.fromFile("Spark.conf").bufferedReader())
  props.load(Source.fromFile("Spark.conf").bufferedReader())
    props.forEach((k, v) => SparkAppConf.set(k.toString, v.toString))
    return SparkAppConf
     //This is a fix for Scala 2.11
    //import scala.collection.JavaConverters._
    //props.asScala.foreach(kv => sparkAppConf.set(kv._1, kv._2))
  
  }
}