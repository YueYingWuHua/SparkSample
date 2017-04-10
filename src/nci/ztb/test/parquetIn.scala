package nci.ztb
package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object parquetIn {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:/hadoop-common")
    val spark = SparkSession.builder.master("local").config(new SparkConf()
      //.setMaster("spark://192.168.12.146:7077")
      .setAppName("sample")).getOrCreate
    import spark.implicits._
    val df = spark.read.parquet("D:/csvOut")
    df.schema.printTreeString()
    val co = df.select("lie1", "lie2", "lie3")
    val co1 = co.select(co("lie3"), $"lie2"+10, $"lie1")
    co1.show()
    //co.write.saveAsTable("ppp")
    val dataf = spark.read.parquet("D:/eclipse/workspace/SparkSample/spark-warehouse/ppp")
    dataf.createOrReplaceTempView("ppp") 
    spark.sql("select * from ppp").show()
    //co.collect().foreach { x => println(x.mkString) }
  }
}