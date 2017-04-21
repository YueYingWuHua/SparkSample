package nci.ztb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer

object SparkSQL {
  
  var tables = new ArrayBuffer[String]
  var path = "D:/jdbcOut"
  val sqlStr = "select * from ${locus} a, ${locus2} b where a.name = b.name"
  
  def analyzeSQL(str: String) = {    
    val par = "\\$\\{[^\\$\\{\\}]+\\}|\\$[^ \t\\$\\{\\}]+" 
    var pattern = Pattern.compile(par)
    var matcher = pattern.matcher(str)
    var sb = new StringBuffer
    while (matcher.find){
      val re = matcher.group
      println(re)
      val r = if (re.startsWith("${")) re.substring(2, re.length - 1) else re.substring(1)      
      matcher.appendReplacement(sb, r)
      tables += r 
    }
    matcher.appendTail(sb)
    sb.toString
  }
  
  def init(args: Array[String]): (String, String) = {
    (args(0), args(1))
  }
  
  
  /**
	 * @param x  use table name x to find the path of table
	 * @return path
 	 */
  def getPath(x: String) = {
    "D:/" + x
  }
  
  def dosql() = {
    System.setProperty("hadoop.home.dir", "D:/hadoop-common")
    val spark = SparkSession.builder.master("local").config(new SparkConf()
      //.setMaster("spark://192.168.12.146:7077")
      .setAppName("sample")
      //.set("spark.local.dir", "D:/sparktmp/")
      .setJars(Array("file:///D:/mysql-connector-java-5.1.41/mysql-connector-java-5.1.41-bin.jar"))
      //.setJars(Array("/home/cloud/mysql-connector-java-5.1.41-bin.jar"))
      ).getOrCreate
    val reader = spark.read
    val sql = analyzeSQL(sqlStr)
    println(sql)
    tables.foreach(x =>{
      val df = reader.parquet(getPath(x))
      df.createOrReplaceTempView(x)
    })
    val sqldf = spark.sql(sql)
    println("the number of result = " + sqldf.count)
    sqldf.show()
    //spark.read.parquet(input).createOrReplaceTempView("")
  }
  
  def main(args: Array[String]): Unit = {
    dosql
  }
}