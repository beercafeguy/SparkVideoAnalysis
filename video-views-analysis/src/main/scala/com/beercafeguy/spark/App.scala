package com.beercafeguy.spark

import com.beercafeguy.spark.commons.{IOUtil, SparkSessionBuilder}
import com.beercafeguy.spark.commons.SparkSessionBuilder.spark.implicits._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * App for data engineering test at Rakutan
  *
  *  1. Assumption Made: DataFrame is a Dataset of Row object so using DataFrame for processing
  *  2. Data is only written disk for one process as this will make resulting zip larger. rest if the processes are doing a show operation
  *
  *
  */


object App {

  val sourceDir = "data/input/sample_dataset/"
  val opDir = "data/output/"
  val deData = sourceDir + "DEvideos.csv"
  val gbData = sourceDir + "GBvideos.csv"
  val inData = sourceDir + "INvideos.csv"
  val usData = sourceDir + "USvideos.csv"


  val file_name=udf((fullPath: String) => fullPath.split("/").last)
  def main(args: Array[String]): Unit = {

    print("Test implementation starts for Rakuten Data Engineering")

    val spark = SparkSessionBuilder.spark

    val sourceDf = IOUtil.readCsv(spark, sourceDir)
      .withColumn("country", substring(file_name(input_file_name()),1,2))
      .cache()


    question_1(sourceDf) //_ used for making it more readable. It should be camel case in scala in general
    question_2(sourceDf)
    question_3(sourceDf)
    question_4(sourceDf)
  }

  /**
    *     1. Compute the following KPI’s using the sample data
    *         a. Find the channels which have most trending video in a country and globally.
    *         b. Find most viewed tag
    *         c. Rank the most liked videos and category across all the dates in a country and globally.
    *
    * @param
    *       source data frame
    */
  def question_1(sourceDf: DataFrame): Unit = {
    // Question One A Process
    question_One_A(sourceDf)
    // Question One B Process
    question_One_B(sourceDf)
    // Question One C Process
    question_One_C(sourceDf)
  }


  /**
    *     2. Consider a like with +1 score, a dislike -5 score, a comment +23 and if comments are disabled then -9999 score.
    *         a. Rank the most favorite (highest total score) channels and videos.
    *         b. Rank the most liked category in a country and globally.
    *         c. Parse the tags into keywords and rank the most favorite (highest total score) keyword in a country and world according to the above score.
    *         d. Find the top 5 most favorite (highest total score), most liked and most disliked keywords across all the dates in a country and globally.
    *
    * @param sourceDf
    */
  def question_2(sourceDf: Dataset[Row]): Unit = {
    val scoreDF = sourceDf
      .withColumn("likes_score", $"likes")
      .withColumn("dislikes_score", $"dislikes" * -5)
      .withColumn("comments_score", $"comment_count" * 23)
      .withColumn("comments_disabled_score", when($"comments_disabled", -9999).otherwise(0))
      .withColumn("score", $"likes_score" + $"dislikes_score" + $"comments_score" + $"comments_disabled_score")
      .cache()
    question_2_a(scoreDF)
    question_2_b(scoreDF)
    question_2_c(scoreDF)
    question_2_d(scoreDF)
  }

  /**
    * Parse the tags into keywords and get the most viewed keyword and most trending keyword in a country and globally.
    *
    * @param sourceDf
    */
  def question_3(sourceDf: DataFrame): Unit = {
    val taggedDf = sourceDf.transform(explodeTags)

    //Parse the tags into keywords and get the
    // most viewed keyword and
    // most trending keyword in a
    // country and
    // globally.
    val aggCountryDF = taggedDf.groupBy($"country", $"tag")
      .agg(sum($"views").as("total_views"), count("*").as("total_trending"))
    val aggAllDF = taggedDf.groupBy($"tag")
      .agg(sum($"views").as("total_views"), count("*").as("total_trending"))

    //most viewed keyword by country
    aggCountryDF.withColumn("row_num", row_number() over (Window.partitionBy($"country").orderBy($"total_views".desc)))
      .filter($"row_num" === 1)
      .drop("row_num", "total_trending")
      .show(false)

    //most viewed keyword globally
    aggAllDF.sort($"total_views".desc)
      .limit(1)
      .drop("total_trending")
      .show(false)


    //most trending keyword by country
    aggCountryDF.withColumn("row_num", row_number() over (Window.partitionBy($"country").orderBy($"total_trending".desc)))
      .filter($"row_num" === 1)
      .drop("row_num", "total_views")
      .show(false)

    //most trending keyword globally
    aggAllDF.sort($"total_trending".desc)
      .limit(1)
      .drop("total_views")
      .show(false)
  }


  /**
    * Rank the most liked keywords per trending date in a country and globally.
    *
    * @param sourceDf
    */
  def question_4(sourceDf: DataFrame): Unit = {
    //Rank the most liked keywords per trending date in a country and globally.
    val taggedDf = sourceDf.transform(explodeTags)

    val countryAgg = taggedDf.groupBy($"trending_date", $"country", $"tag")
      .agg(sum($"likes").as("total_likes"))
    val allAgg = taggedDf.groupBy($"trending_date", $"tag")
      .agg(sum($"likes").as("total_likes"))

    //most liked keywords per trending date in a country
    countryAgg.withColumn("row_num", row_number() over (Window.partitionBy($"country", $"trending_date").orderBy($"total_likes".desc)))
      .filter($"row_num" === 1)
      .drop("row_num")
      .sort($"trending_date")
      .show(100, false)

    //most liked keywords per trending date globally
    allAgg.withColumn("row_num", row_number() over (Window.partitionBy($"trending_date").orderBy($"total_likes".desc)))
      .filter($"row_num" === 1)
      .drop("row_num")
      .sort($"trending_date")
      .show(100, false)


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
    dataFrame.groupBy($"country", $"category_id")
      .agg(sum($"likes").as("total_likes")) // asuming liked is based on likes only
      .withColumn("row_num", row_number() over (Window.partitionBy($"country").orderBy($"total_likes".desc)))
      .filter($"row_num" === 1)
      .drop("row_num")
      .show(false)
    //most liked category globally

    dataFrame.groupBy($"category_id")
      .agg(sum($"likes").as("total_likes")) // asuming liked is based on likes only
      .withColumn("row_num", row_number() over (Window.orderBy($"total_likes".desc)))
      .filter($"row_num" === 1)
      .drop("row_num")
      .show(false)
  }

  def question_2_c(dataFrame: DataFrame): Unit = {
    //tag in country
    dataFrame.transform(explodeTags)
      .groupBy($"country", $"tag")
      .agg(sum($"score").as("total_score"))
      .withColumn("row_num", row_number() over (Window.partitionBy($"country").orderBy($"total_score".desc)))
      .filter($"row_num" === 1)
      .show(false)

    //tag all over world
    dataFrame.transform(explodeTags)
      .groupBy($"tag")
      .agg(sum($"score").as("total_score"))
      .withColumn("row_num", row_number() over (Window.orderBy($"total_score".desc)))
      .show(false)


  }

  def question_2_d(dataFrame: DataFrame): Unit = {
    // Find the top 5 most favorite (highest total score), most liked and
    // most disliked keywords across all the dates in a country and globally.


    val tagged = dataFrame.transform(explodeTags)

    val countryAggDF = tagged.groupBy($"country", $"tag")
      .agg(sum($"score").as("total_score"), sum($"likes").as("total_likes"), sum($"dislikes").as("total_dislikes"))

    val globeAggDF = tagged.groupBy($"tag")
      .agg(sum($"score").as("total_score"), sum($"likes").as("total_likes"), sum($"dislikes").as("total_dislikes"))

    //most favorite per country
    countryAggDF.withColumn("row_num", row_number() over (Window.partitionBy($"country").orderBy($"total_score".desc)))
      .filter($"row_num" <= 5)
      .drop("row_num", "total_likes", "total_dislikes")
      .show(100, false)


    //most favorite global
    globeAggDF.sort($"total_score".desc)
      .limit(5)
      .drop("total_likes", "total_dislikes")
      .show(false)

    //most liked per country
    countryAggDF.withColumn("row_num", row_number() over (Window.partitionBy($"country").orderBy($"total_likes".desc)))
      .filter($"row_num" <= 5)
      .drop("row_num", "total_score", "total_dislikes")
      .show(100, false)


    //most liked global
    globeAggDF.sort($"total_likes".desc)
      .limit(5)
      .drop("total_dislikes", "total_score")
      .show(false)

    //most disliked per country
    countryAggDF.withColumn("row_num", row_number() over (Window.partitionBy($"country").orderBy($"total_dislikes".desc)))
      .filter($"row_num" <= 5)
      .drop("row_num", "total_score", "total_likes")
      .show(100, false)


    //most disliked global
    globeAggDF.sort($"total_dislikes".desc)
      .limit(5)
      .drop("total_likes", "total_score")
      .show(false)
  }




  def explodeTags(df: DataFrame): DataFrame = {
    df.withColumn("tags_array", split($"tags", "\\|"))
      .withColumn("tag", explode($"tags_array"))
      .withColumn("tag", regexp_replace($"tag", "\"", ""))
  }
}
