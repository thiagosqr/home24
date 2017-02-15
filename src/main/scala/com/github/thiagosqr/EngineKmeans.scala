package com.github.thiagosqr

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{explode, udf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParArray


/**
  * Created by thiago on 13/02/17.
  */
object EngineKmeans extends App{

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Engine")
    .set("spark.executor.memory", "2g")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  import spark.implicits._

//  val data = spark.read.json("src/main/resources/test.json")
//    data.createOrReplaceTempView("skus")
//
//    val buffer = ListBuffer.empty[SkuKmeans]
//
//    data.columns.foreach { c =>
//
//      val sku = data.select(data.col(c)).map(toSku(_,c)).first()
//      buffer += sku
//
//      if(buffer.length >= 1000){
//        write(buffer)
//        buffer.clear()
//      }
//    }
//
//    if(buffer.length > 0){
//      write(buffer)
//      buffer.clear()
//    }

  val df = spark.read.parquet("skus_kmeans.parquet").as[SkuKmeans]
  df.createOrReplaceTempView("skus")
  //  df.show()
  //  df.printSchema()

  val input = "sku-123"
  val sku = df.filter(s => s.id == input).first()

  if(sku != null){

    val vectors = df.rdd.map(r => Vectors.dense( r.att_a, r.att_b, r.att_c))
    vectors.cache()

    val kMeansModel = KMeans.train(vectors, 2, 20)

    kMeansModel.clusterCenters.foreach(println)



  }

  def toSku(r: Row, col: String):SkuKmeans = {

    val row = r.getAs[Row](0)

    SkuKmeans(col, toDbl(row.getAs[String]("att-a")), toDbl(row.getAs[String]("att-b")),
     toDbl(row.getAs[String]("att-c")), toDbl(row.getAs[String]("att-d")), toDbl(row.getAs[String]("att-e")),
     toDbl(row.getAs[String]("att-f")), toDbl(row.getAs[String]("att-g")), toDbl(row.getAs[String]("att-h")),
     toDbl(row.getAs[String]("att-i")), toDbl(row.getAs[String]("att-j")))
  }

  def toDbl(s: String) = s.replaceAll("[^0-9]","").toDouble

  def howSimiliar(skua: Sku, skub: Sku):Int = 10

  def write(b: ListBuffer[SkuKmeans]):Unit = {
    val rdd = spark.sparkContext.parallelize(b)
    val df2: DataFrame = spark.createDataFrame(rdd)
    df2.write.mode(SaveMode.Append).parquet("skus_kmeans.parquet")
  }

}

case class SkuKmeans(id: String, att_a: Double, att_b: Double, att_c: Double, att_d: Double, att_e: Double,
               att_f: Double, att_g: Double, att_h: Double, att_i: Double, att_j: Double)


//  val attrs = Seq("att-a","att-b","att-c","att-d","att-e","att-f","att-g","att-h","att-i","att-j")
//  val columns =  data.columns.map(data.col(_))
//  data.groupBy(columns: _*).pivot("att", attrs).count()
