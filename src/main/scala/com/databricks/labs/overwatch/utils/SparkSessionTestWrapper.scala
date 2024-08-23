package com.databricks.labs.overwatch.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper extends SparkSessionWrapper {

  override lazy protected val _envInit: Boolean = true

  override private[overwatch] def spark( globalSession: Boolean = true): SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionTestWrapper")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  @transient override lazy val spark: SparkSession = spark(true)

  override lazy val sc: SparkContext = spark.sparkContext

  override def envInit( logLevel: String): Boolean = {
    sc.setLogLevel( logLevel)
    true

  }

}
