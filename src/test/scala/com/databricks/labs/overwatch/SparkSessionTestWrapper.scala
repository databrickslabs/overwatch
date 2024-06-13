package com.databricks.labs.overwatch

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest._

trait SparkSessionTestWrapper extends BeforeAndAfterEach {

  this: TestSuite =>

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext

  override def beforeEach(): Unit = {
    sc.setLogLevel( "WARN")
    // in case of any stackable traits:
    super.beforeEach()
  }

}
