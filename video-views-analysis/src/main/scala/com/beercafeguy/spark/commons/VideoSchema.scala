package com.beercafeguy.spark.commons

import org.apache.spark.sql.types._

object VideoSchema {

  val givenSchema=new StructType()
    .add(StructField("video_id",StringType))
    .add(StructField("trending_date",StringType))
    .add(StructField("title",StringType))
    .add(StructField("channel_title",StringType))
    .add(StructField("category_id",LongType))
    .add(StructField("publish_time",TimestampType))
    .add(StructField("tags",StringType))
    .add(StructField("views",LongType))
    .add(StructField("likes",LongType))
    .add(StructField("dislikes",LongType))
    .add(StructField("comment_count",LongType))
    .add(StructField("comments_disabled",BooleanType))
    .add(StructField("ratings_disabled",BooleanType))
    .add(StructField("video_error_or_removed",BooleanType))



}
