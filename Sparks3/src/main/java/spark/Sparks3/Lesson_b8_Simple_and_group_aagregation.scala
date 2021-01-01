package spark.Sparks3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Lesson_b8_Simple_and_group_aagregation {
  def lesson8()
  {
    val spark = SparkSession.builder()
      .appName("Simple and grouping Aggregation")
      .master("local[3]")
      .getOrCreate()
      
      val DF=spark.read
      .option("header", "true")
      .option("InferSchema","true")
      .csv("D:\\Bigdata\\invoices.csv")
    DF//.show
 
    
    //Simple Aggragation
      //Using SQL Expression
  DF.selectExpr("count(*) as count","sum(Quantity) as TotalQuantity","avg(UnitPrice) as AvgPrice","count(StockCode) as CountField")
  //.show
//Using  Column Object
  DF.select(count("*").as("count"),sum("Quantity").as("TotalQuantity"),avg("UnitPrice").as("AvgPrice"),count("StockCode").as("CountField"))
  //.show
  
  //Grouping Aggregation
  
  //Using SQL Expression
  DF.createOrReplaceTempView("sales")
  spark.sql("""
    |select country,InvoiceNo ,
    |sum(Quantity) as TotalQuantity,
    |round(sum(Quantity*UnitPrice),2) as InvoiceValue
    |from sales
    |group by country,InvoiceNo
    """.stripMargin)//.show
    
    //Using Column Object
    DF.groupBy("country", "InvoiceNo")
    .agg((sum("Quantity").as("Total Count")),      
    round(sum(expr("Quantity*UnitPrice")), 2).as("InvoiceValue")
    //OR// expr("round(sum(Quantity*UnitPrice),2) as InvoiceValueExpr")
    )//.show()
    
    
    //Grouping Example
    //InvoiceNo|StockCode|         Description|Quantity|    InvoiceDate|UnitPrice|CustomerID|       Country
     
    //You may write Expression like this for the clean code
    val NumInvoices = countDistinct("InvoiceNo").as("NumInvoices")
    val TotalQuantity = sum("Quantity").as("TotalQuantity")
    val InvoiceValue = expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")
    
    val DF2=DF.withColumn("InvoiceDate", to_date(col("InvoiceDate"),"dd-MM-yyyy H.mm"))
    .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
    .groupBy("Country", "WeekNumber")
    .agg(NumInvoices,TotalQuantity,InvoiceValue)
   // .show
    
    DF2.coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save("D:\\Bigdata\\aggregation\\")

    
  }
}

