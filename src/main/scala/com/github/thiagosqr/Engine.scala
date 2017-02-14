package com.github.thiagosqr

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{explode, udf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParArray


/**
  * Created by thiago on 13/02/17.
  */
object Engine extends App{

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Engine")
    .set("spark.executor.memory", "2g")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  import spark.implicits._

//  val data = spark.read.json("src/main/resources/test.json")
//  data.createOrReplaceTempView("skus")
//
//  val buffer = ListBuffer.empty[Sku]
//
//  data.columns.foreach { c =>
//
//    val sku = data.select(data.col(c)).map(toSku(_,c)).first()
//    buffer += sku
//
//    if(buffer.length >= 1000){
//      write(buffer)
//      buffer.clear()
//    }
//  }
//
//  if(buffer.length > 0){
//    write(buffer)
//    buffer.clear()
//  }

  val df = spark.read.parquet("skus.parquet").as[Sku]
  df.createOrReplaceTempView("skus")
  df.show()
  df.printSchema()

  val input = "sku-123"
  val sku = df.filter(s => s.id == input).first()


  def toSku(r: Row, col: String):Sku = {

    val row = r.getAs[Row](0)

    Sku(col, row.getAs[String]("att-a"), row.getAs[String]("att-b"), row.getAs[String]("att-c"),
        row.getAs[String]("att-d"), row.getAs[String]("att-e"), row.getAs[String]("att-f"), row.getAs[String]("att-g"),
        row.getAs[String]("att-h"), row.getAs[String]("att-i"), row.getAs[String]("att-j"))
  }

  def howSimiliar(skua: Sku, skub: Sku):Int = 10

  def write(b: ListBuffer[Sku]):Unit = {
    val rdd = spark.sparkContext.parallelize(b)
    val df2: DataFrame = spark.createDataFrame(rdd)
    df2.write.mode(SaveMode.Append).parquet("skus.parquet")
  }

}

case class Sku(id: String, att_a: String, att_b: String, att_c: String, att_d: String, att_e: String,
               att_f: String, att_g: String, att_h: String, att_i: String, att_j: String)


//  val attrs = Seq("att-a","att-b","att-c","att-d","att-e","att-f","att-g","att-h","att-i","att-j")
//  val columns =  data.columns.map(data.col(_))
//  data.groupBy(columns: _*).pivot("att", attrs).count()
