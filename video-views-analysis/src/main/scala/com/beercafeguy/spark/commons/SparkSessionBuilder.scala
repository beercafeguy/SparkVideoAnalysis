package com.beercafeguy.spark.commons

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {


  val spark=SparkSession.builder()
    .master("local[*]")
    .appName("VideoAnalyzer")
    .config("spark.sql.shuffle.partitions","16")
    .getOrCreate()

}
