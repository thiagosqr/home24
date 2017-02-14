package com.github.thiagosqr

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by thiago on 13/02/17.
  */
object Engine extends App{

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Engine")
    .set("spark.executor.memory", "2g")
    .set("spark.storage.memoryFraction", "0.5")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  val data = spark.read.json("src/main/resources/test.json")

  //Filter rows with all attrs equal
  data.filter(r => r.)


  data.show(50)
  data.printSchema()

}
