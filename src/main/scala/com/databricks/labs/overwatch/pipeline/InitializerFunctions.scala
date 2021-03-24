package com.databricks.labs.overwatch.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

object InitializerFunctions {
  /**
   * Load text file
   * @param path path to the local resource
   * @return sequence of lines read from the file
   */
  def loadLocalResource(path: String): Seq[String] = {
    val fileLocation = getClass.getResourceAsStream(path)
    if (fileLocation == null)
      throw new RuntimeException(s"There is no resource at path: $path")
    val source = scala.io.Source.fromInputStream(fileLocation).mkString.stripMargin
    source.split("\\r?\\n").filter(_.trim.nonEmpty).toSeq
  }

  /**
   * Load database for cloud provider node details
   * @param path path to the local resource
   * @return
   */
  def loadLocalCSVResource(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    val csvData = loadLocalResource(path).toDS()
    spark.read.option("header", "true").option("inferSchema", "true").csv(csvData).coalesce(1)
  }
}
