package spark.Sparks3

import spark.Sparks3._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object ExecutorClass {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(agrs:Array[String])
  {

     //Lesson_a0_Starting_SparkSession.lesson0()
     //Lesson_a1_Configure_Spark_application.lesson1()
     //Lesson_a2_Reading_Data_csv.lesson2()
     //Lesson_a3_Transformations_in_Dataframe.lesson3()
     //Lesson_a4_JOB_Stages_Task.lesson4()
     //Lesson_a5_Hello_Spark_testing.lesson5()
     //Lesson_a6_RDD_Api.lesson6()
     //Lesson_a7_Working_with_Dataset.Lesson7()
     //Lesson_a8_Spark_SQL.Lesson8()
     //Lesson_a9_File_format_reading.lesson9()
     //Lesson_b0_Creatting_Schema.lesson0()
     //Lesson_b1_Dataframewriter.lesson1()
     Lesson_b2_Spark_Hive_metastore.lession2()
     //Lesson_b3_Dataframetransformation1.lesson3()
     //Lesson_b4_low_level_fucn_unstructure_data_handling.lesson4()
     //Lesson_b5_Column_Transformation.lesson5()
     //Lesson_b6_Working_with_UDF.Lesson6()
     //Lesson_b7_Misc_transformation.lesson7()
     //Lesson_b8_Simple_and_group_aagregation.lesson8()
     //Lesson_b9_WindowingAggregation.lesson9()
     //Lesson_c0_Ranking.Lesson0
     //Lesson_c1_Spark_join.Lesson1()
     //Lesson_c2_OuterJoin.Lesson2()
     //Lesson_c3_Shuffle_join.Lesson3()
     Lesson_c4_bucket_join.Lesson4()
     //Lesson_c5_Complex_type.Lesson5()
  }
     
}