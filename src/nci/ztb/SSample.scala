package nci.ztb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.Properties
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer


object SSample {
  
  def init(args: Array[String]): (String, String, String, String, String) = {
    (args(0), args(1), args(2), args(3), args(4))
  }
  
  def matchType(s: String): DataType = s.toLowerCase match {
    case "string" | "pic" => StringType
    case "int" | "integer" => IntegerType
    case "date" => DateType
    case "long" => LongType 
    case "short" => ShortType
    case "float" => FloatType
    case "double" => DoubleType
    case _ => StringType
  }
  
  def makeStruct(columns: String) = {
    var sf : List[StructField] = List()
    val column = columns.split(",|\\s+")
    for (i <- 0 until column.length; if (i % 2 == 0))
      sf :+= StructField(column(i), matchType(column(i+1))) 
    StructType(sf)
  }
  
  //5个参数，数据地址，表名，数据类型，抽样列, 全部列名和列类型
  def main(args: Array[String]): Unit = {
    //D:/testcsv.txt TCSV cSv lie1 lie1,lie2,lie3,lie4,lie5
    //jdbc:mysql://192.168.12.222:3306/test locus mysql l123qasd 123
    //col测试用，正式版删除
    var col = new Array[String](70)
    for (i <- 0 until 70; if (i % 2 == 0)){
      col(i) = "lie" + i/2
      if (i < 18) col(i+1) = "integer" else col(i+1) = "string"
    }
    System.setProperty("hadoop.home.dir", "D:/hadoop-common")
    val (url, tableName, sourceType, sampleColumns, columns) = init(args)
    println(columns)
    val spark = SparkSession.builder.master("spark://192.168.12.147:7077").config(new SparkConf()
      //.setMaster("spark://192.168.12.146:7077")
      .setAppName("sample")
      //.set("spark.local.dir", "D:/sparktmp/")
      .setJars(Array("file:///D:/mysql-connector-java-5.1.41/mysql-connector-java-5.1.41-bin.jar"))
      //.setJars(Array("/home/cloud/mysql-connector-java-5.1.41-bin.jar"))
      ).getOrCreate
    val reader = spark.read    
    val df = sourceType.toLowerCase match {
      case "mysql" => {
        val prop = new Properties()
        prop.setProperty("user", "root")
        prop.setProperty("password", "Dx72000000!")
        reader.jdbc(url, tableName, prop)        
      }
      case "parquet" => reader.parquet(url)
      case "orc" => reader.orc(url)
      case "csv" => {
       val struct = makeStruct(col.mkString(","))
       reader.schema(struct).csv(url)
      }
    }
    //val st = df.schema.iterator
    df.columns.foreach { println }
    df.write.parquet("D:/WorkSpaceBDP2edition/jdbcOut")
    spark.stop
  }
}