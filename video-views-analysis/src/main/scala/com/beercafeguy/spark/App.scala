package com.beercafeguy.spark

import com.beercafeguy.spark.commons.{IOUtil, SparkSessionBuilder}
import com.beercafeguy.spark.commons.SparkSessionBuilder.spark.implicits._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Hello world!
  *
  */

// Assumption Made: DataFrame is a Dataset of Row object so using DataFrame for processing
object App {

  val sourceDir = "data/input/sample_dataset/"
  val opDir = "data/output/"
  val deData = sourceDir + "DEvideos.csv"
  val gbData = sourceDir + "GBvideos.csv"
  val inData = sourceDir + "INvideos.csv"
  val usData = sourceDir + "USvideos.csv"


  def main(args: Array[String]): Unit = {

    print("Test implementation starts for Rakuten Data Engineering")

    val spark = SparkSessionBuilder.spark

    val deDF = IOUtil.readCsv(spark, deData)
      .withColumn("country", lit("DE"))
    val gbDF = IOUtil.readCsv(spark, gbData)
      .withColumn("country", lit("GB"))
    val inDF = IOUtil.readCsv(spark, inData)
      .withColumn("country", lit("IN"))
    val usDF = IOUtil.readCsv(spark, usData)
      .withColumn("country", lit("US"))
    val sourceDf = (deDF union gbDF union inDF union usDF).cache()

    // Question One A Process
    //question_One_A(sourceDf) // _ used for making it more readable. It should be camel case in scala in general

    // Question One B Process
    // question_One_B(sourceDf)

    // Question One C Process
    // question_One_C(sourceDf)

    // Question 2
    question2(sourceDf)
  }

  def question2(sourceDf: Dataset[Row]): Unit = {
    val scoreDF = sourceDf
      .withColumn("likes_score", $"likes")
      .withColumn("dislikes_score", $"dislikes" * -5)
      .withColumn("comments_score", $"comment_count" * 23)
      .withColumn("comments_disabled_score", when($"comments_disabled", -9999).otherwise(0))
      .withColumn("score", $"likes_score" + $"dislikes_score" + $"comments_score" + $"comments_disabled_score")
      .cache()
    //question_2_a(scoreDF)
    //question_2_b(scoreDF)
    question_2_c(scoreDF)
    //scoreDF.show(false)
  }

  def question_2_a(dataFrame: DataFrame): Unit = {
    //favourite channel
    dataFrame.groupBy($"channel_title")
      .agg(sum($"score").as("total_score"))
      .sort($"total_score".desc)
      .limit(1)
      .drop("total_score")
      .show(false)
    //favourite video
    dataFrame.groupBy($"video_id")
      .agg(sum($"score").as("total_score"))
      .sort($"total_score".desc)
      .limit(1)
      .drop("total_score")
      .show(false)
  }

  def question_2_b(dataFrame: DataFrame): Unit = {
    //most liked category in a country
    dataFrame.groupBy($"country",$"category_id")
      .agg(sum($"likes").as("total_likes")) // asuming liked is based on likes only
      .withColumn("row_num",row_number() over (Window.partitionBy($"country").orderBy($"total_likes".desc)))
      .filter($"row_num"===1)
      .drop("row_num")
      .show(false)
    //most liked category globally

    dataFrame.groupBy($"category_id")
      .agg(sum($"likes").as("total_likes")) // asuming liked is based on likes only
      .withColumn("row_num",row_number() over (Window.orderBy($"total_likes".desc)))
      .filter($"row_num"===1)
      .drop("row_num")
      .show(false)
  }

  def question_2_c(dataFrame: DataFrame): Unit = {
    //tag in country
    dataFrame.withColumn("tags_array",split($"tags", "\\|"))
      .withColumn("tag",explode($"tags_array"))
      .withColumn("tag",regexp_replace($"tag","\"",""))
      .groupBy($"country",$"tag")
      .agg(sum($"score").as("total_score"))
      .withColumn("row_num",row_number()  over(Window.partitionBy($"country").orderBy($"total_score".desc)))
      .filter($"row_num"===1)
      .show(false)

    //tag all over world
    dataFrame.withColumn("tags_array",split($"tags", "\\|"))
      .withColumn("tag",explode($"tags_array"))
      .withColumn("tag",regexp_replace($"tag","\"",""))
      .groupBy($"tag")
      .agg(sum($"score").as("total_score"))
      .withColumn("row_num",row_number()  over(Window.orderBy($"total_score".desc)))
      .show(false)


  }

  def question_2_d(dataFrame: DataFrame):Unit={
    // Find the top 5 most favorite (highest total score), most liked and
    // most disliked keywords across all the dates in a country and globally.


    val tagged=dataFrame.withColumn("tags_array",split($"tags", "\\|"))
      .withColumn("tag",explode($"tags_array"))
      .withColumn("tag",regexp_replace($"tag","\"",""))
    val countryAggDF=tagged.groupBy($"country",$"tag")
      .agg(sum($"score").as("total_score"),sum($"likes").as("total_likes"),sum($"dislikes").as("total_dislikes"))

    val globeAggDF=tagged.groupBy($"tag")
      .agg(sum($"score").as("total_score"),sum($"likes").as("total_likes"),sum($"dislikes").as("total_dislikes"))

    //most favorite per country
    countryAggDF.withColumn("row_num",row_number() over(Window.partitionBy($"country").orderBy($"total_score".desc)))
      .filter($"row_num"===1)
      .drop($"row_num")
      .show(false)


    //most favorite per country
    countryAggDF.withColumn("row_num",row_number() over(Window.partitionBy($"country").orderBy($"total_score".desc)))
      .filter($"row_num"===1)
      .drop($"row_num")
      .show(false)
  }
  def question_One_C(sourceDf: Dataset[Row]): Unit = {
    //most liked video ranking country wise
    sourceDf.groupBy($"country", $"video_id")
      .agg(sum($"likes").as("total_likes"))
      .withColumn("rnk", rank() over (Window.partitionBy("country").orderBy($"total_likes".desc)))
      .show(false)

    //most likes category ranking country wise
    sourceDf.groupBy($"country", $"category_id")
      .agg(sum($"likes").as("total_likes"))
      .withColumn("rnk", rank() over (Window.partitionBy("country").orderBy($"total_likes".desc)))
      .show(false)

    //most liked video ranking Globally
    sourceDf.groupBy($"video_id")
      .agg(sum($"likes").as("total_likes"))
      .withColumn("rnk", rank() over (Window.orderBy($"total_likes".desc)))
      .show(false)

    //most likes category ranking Globally
    sourceDf.groupBy($"category_id")
      .agg(sum($"likes").as("total_likes"))
      .withColumn("rnk", rank() over (Window.orderBy($"total_likes".desc)))
      .show(false)
  }

  def question_One_B(sourceDf: DataFrame): Unit = {
    sourceDf.select(split($"tags", "\\|").as("tags"), $"views")
      .select(explode($"tags").as("tag"), $"views")
      .select(regexp_replace($"tag", "\"", "").as("tag"), $"views")
      .groupBy("tag")
      .agg(sum($"views").as("total_views"))
      .sort($"total_views".desc)
      .drop("count")
      .limit(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(opDir + "/popular_tag/")
  }

  def question_One_A(sourceDf: DataFrame): Unit = {
    val popularChannelGloble = sourceDf.groupBy($"channel_title").count.sort($"count".desc)
      .limit(1)
      .drop("count")
    popularChannelGloble.write
      .mode(SaveMode.Overwrite)
      .parquet(opDir + "/popular_globle_channel/")

    val countSpec = Window.partitionBy($"country", $"channel_title")
    val rowNumSpec = Window.partitionBy($"country").orderBy($"country_wise_count".desc)
    val popularCountryWise = sourceDf
      .select($"country", $"channel_title")
      .withColumn("country_wise_count", count(lit(1)) over (countSpec))
      .groupBy($"country", $"channel_title")
      .agg(max($"country_wise_count").as("country_wise_count"))
      .withColumn("row_num", row_number over rowNumSpec)
      .filter($"row_num" === 1)
      .drop("row_num")


    popularCountryWise.write
      .mode(SaveMode.Overwrite)
      .parquet(opDir + "/popular_channel_country_wise/")
  }
}
