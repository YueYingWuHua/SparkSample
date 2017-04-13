package nci.ztb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.Properties
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders


object SSample {
  
  def init(args: Array[String]): (String, String, String, String, Integer, String) = {
    (args(0), args(1), args(2), args(3), args(4).toInt, args(5))
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
  
  def mkProperties(user: String, pw: String) = {
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "Dx72000000!")
    prop.setProperty("useUnicode", "true")
    prop.setProperty("characterEncoding", "utf-8")
    prop
  }
  
  def mkStruct(columns: String) = {
    var sf : List[StructField] = List()
    val column = columns.split(",|\\s+")
    for (i <- 0 until column.length; if (i % 2 == 0))
      sf :+= StructField(column(i), matchType(column(i+1))) 
    StructType(sf)
  }
  
  def doSample(args: Array[String]) = {    
    //col测试用，正式版删除
    var col = new Array[String](70)
    for (i <- 0 until 70; if (i % 2 == 0)){
      col(i) = "lie" + i/2
      if (i < 18) col(i+1) = "integer" else col(i+1) = "string"
    }
    System.setProperty("hadoop.home.dir", "D:/hadoop-common")
    //先这么测试输入参数
    require(args.length == 6)
    var (url, tableName, sourceType, sampleColumns, amount, columns) = init(args)
    println(columns)
    //创建sparkSessiong
    val spark = SparkSession.builder.master("local").config(new SparkConf()
      //.setMaster("spark://192.168.12.146:7077")
      .setAppName("sample")
      //.set("spark.local.dir", "D:/sparktmp/")
      .setJars(Array("file:///D:/mysql-connector-java-5.1.41/mysql-connector-java-5.1.41-bin.jar"))
      //.setJars(Array("/home/cloud/mysql-connector-java-5.1.41-bin.jar"))
      ).getOrCreate
    val reader = spark.read
    //从源地址读取数据
    val df = sourceType.toLowerCase match {
      case "mysql" => reader.jdbc(url, tableName, mkProperties("root", "Dx72000000!"))        
      case "parquet" => reader.parquet(url)
      case "orc" => reader.orc(url)
      case "csv" => reader.schema(mkStruct(col.mkString(","))).csv(url)
    }
    //解析抽样列
    val scs = sampleColumns.split(",|\\s+")
    val count = df.count
    //用sample需要先取多一点数据再在小数据集抽样，但是take去前N个会导致随机性不足，所以不用这种方法val fraction = Math.min(amount.toDouble*1000/count, 0.85)
    if (count < amount) amount = count.toInt
    //将抽样列数组转化为column*，并取出这些列，并对其进行抽样，由于sample是基于Bernoulli sampling方法或者Poisson sampling方法，所以不能用sample，需要先转化为RDD进行操作
    val arr = df.select(scs.map(df(_)):_*).rdd.takeSample(false, amount)//.sample(false, fraction).take(amount)
    //将取出来的数据转化回DataFrame，用来写parquet和jdbc
    val rowRDD = spark.sparkContext.makeRDD(arr)
    assert(arr.length >= 1)
    //输出模块，jdbc修改为用户密码指定的
    val writer = spark.createDataFrame(rowRDD, arr(0).schema).write
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    //写抽样数据数据库
    writer.jdbc(url, "Sample"+ tableName + sdf.format(new Date(System.currentTimeMillis())), mkProperties("root", "Dx72000000!"))
    //写抽样数据库
    
    
    //writer.parquet("D:/jdbcOut")
    spark.stop
  }
  
  //5个参数，数据地址，表名，数据类型，抽样列, fraction,全部列名和列类型
  def main(args: Array[String]): Unit = {
    //D:/testcsv.txt TCSV cSv lie1 lie1,lie2,lie3,lie4,lie5
    //jdbc:mysql://192.168.12.222:3306/test locus mysql l123qasd 123
    //写数据库抽样开始
    def writeStart = {
      
    }
    writeStart
    
    try{
      doSample(args)
    }catch{
      case e: Throwable => e.printStackTrace 
    }finally{
      //写数据库抽样结束
      def writeStop = {
        
      }
      writeStop 
    }
  }
}