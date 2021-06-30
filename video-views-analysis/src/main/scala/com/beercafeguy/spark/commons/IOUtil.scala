package com.beercafeguy.spark.commons

import org.apache.spark.sql.{DataFrame, SparkSession}

object IOUtil {

  def readCsv(spark: SparkSession, location: String): DataFrame = {
    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(VideoSchema.givenSchema)
      .csv(location)
  }
}
