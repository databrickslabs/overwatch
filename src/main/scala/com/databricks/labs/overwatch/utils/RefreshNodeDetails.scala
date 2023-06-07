package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.pipeline.Pipeline
import com.databricks.labs.overwatch.utils.RefreshNodeDetails.isMultiCloud
import io.delta.tables.DeltaTable
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

private class RefreshNodeDetails(
                                  spark: SparkSession,
                                  etlDB: String,
                                  cloud: String,
                                  workspaceCloudMap: Map[String, String],
                                  tempWorkingDirPrefix: String
                                ) {

  private val refreshSnapTime: TimeTypes = Pipeline.createTimeDetail(System.currentTimeMillis())

  import spark.implicits._
  private val sc = spark.sparkContext
//  private val globalOrgCloudDF
  private def isMultiCloud: Boolean = RefreshNodeDetails.isMultiCloud(cloud, workspaceCloudMap)
  private val _tempWorkingDirRaw: String = s"$tempWorkingDirPrefix/${refreshSnapTime.asUnixTimeMilli.toString}"
  private val tempWorkingDir: String = Helpers.sanitizeURL(_tempWorkingDirRaw)
  private val distinctClouds: Array[String] = if (isMultiCloud) workspaceCloudMap.values.toArray.distinct else Array[String](cloud)

  private val rawPipReportDF: DataFrame = spark.table(s"$etlDB.pipeline_report")
    .repartition().cache()
  rawPipReportDF.count()

  private val scopedOrgIds: Array[String] = if (isMultiCloud) {
    workspaceCloudMap.keys.toArray
  } else rawPipReportDF.select('organization_id).distinct.as[String].collect()

  private val scopedOrgIdsFilter: Column = col("organization_id").isin(scopedOrgIds: _*)

  private val scopedPipReportDF = rawPipReportDF
    .filter(scopedOrgIdsFilter)

  private def getStaticCloudResourceLink(cloud: String): String = {
    cloud.toLowerCase match {
      case "azure" => "https://raw.githubusercontent.com/databrickslabs/overwatch/main/src/main/resources/Azure_Instance_Details.csv"
      case "aws" => "https://raw.githubusercontent.com/databrickslabs/overwatch/main/src/main/resources/AWS_Instance_Details.csv"
      case "gcp" => "https://raw.githubusercontent.com/databrickslabs/overwatch/main/src/main/resources/Gcp_Instance_Details.csv"
      case _ => throw new Exception(s"Supported Cloud Types are azure|aws|gcp")
    }
  }

  private def getLocalStaticResourceFileName(cloud: String): String = {
    cloud.toLowerCase match {
      case "azure" => "Azure_Instance_Details.csv"
      case "aws" => "AWS_Instance_Details.csv"
      case "gcp" => "Gcp_Instance_Details.csv"
      case _ => throw new Exception(s"Supported Cloud Types are azure|aws|gcp")
    }
  }

  // returns df of organization_id, primordialDateString, cloud
  private def globalOrgCloudPrimordialDF: DataFrame = {

    val primordialW = Window.partitionBy('organization_id).orderBy('Pipeline_SnapTS.desc)
    val latestScopedMetaBase = scopedPipReportDF
      .withColumn("rnk", rank().over(primordialW))
      .withColumn("rn", row_number().over(primordialW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
      .select('organization_id, 'primordialDateString)
      .distinct()

    if (isMultiCloud) {
      workspaceCloudMap.toSeq.toDF("organization_id", "cloud")
        .join(latestScopedMetaBase, Seq("organization_id"))
    } else {
      latestScopedMetaBase
        .withColumn("cloud", lit(cloud))
    }

  }

  private def landStaticResourceFiles(): Unit = {
    distinctClouds.foreach(c => {
      val url = getStaticCloudResourceLink(c)
      sc.addFile(url)
      val fileName = getLocalStaticResourceFileName(c)
      val staticResourcePath = SparkFiles.get(fileName)
      dbutils.fs.cp(s"file://$staticResourcePath", s"$tempWorkingDir/$fileName")
    })
  }

  private def metaFillerDF: DataFrame = {

    spark.table(s"$etlDB.instanceDetails")
      .select('organization_id, 'isActive, 'Pipeline_SnapTS, lit("manual").alias("Overwatch_RunID"), 'workspace_name)
      .withColumn("rnk", rank().over(w))
      .withColumn("rn", row_number().over(w))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
      .withColumn("activeUntil", lit(null).cast("date"))
      .withColumn("Pipeline_SnapTS", refreshSnapTime.asColumnTS)

  }


//  val url = "https://raw.githubusercontent.com/databrickslabs/overwatch/main/src/main/resources/AWS_Instance_Details.csv"
//  sc.addFile(url)
//
//  val path = SparkFiles.get("AWS_Instance_Details.csv")
//  dbutils.fs.cp(s"file://$path", targetTempPath)
//
//  val existingOrgIDs = spark.table(s"$etlDB.instanceDetails").select('organization_id).distinct.as[String].collect()
//
//  // build meta data fields to be inserted when keys not matched. When they are matched, these fields are not updated
//  val w = Window.partitionBy('organization_id).orderBy('Pipeline_SnapTS.desc)
//  val metaFillerDF = spark.table(s"$etlDB.instanceDetails")
//    .select('organization_id, 'activeFrom, 'activeUntil, 'isActive, 'Pipeline_SnapTS, lit("manual").alias("Overwatch_RunID"), 'workspace_name)
//    .withColumn("rnk", rank().over(w))
//    .withColumn("rn", row_number().over(w))
//    .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
//    .withColumn("Pipeline_SnapTS", current_timestamp)
//
//  val updatesDF = spark.read.option("header", "true").option("inferSchema", "true").format("csv")
//    .load(targetTempPath)
//    .withColumn("organization_id", explode(lit(existingOrgIDs)))
//    .join(metaFillerDF, Seq("organization_id"), "left")
//    //   .withColumn("instance", lit(null).cast("string")) // only necessary if multi-cloud deployment
//    .alias("updates")
//
//  val deltaTarget = DeltaTable.forName(s"$etlDB.instanceDetails").alias("target")
//
//  val mergeCondition =
//    """
//  updates.organization_id = target.organization_id AND
//  updates.API_Name = target.API_Name
//  """


}

object RefreshNodeDetails {

  private def isMultiCloud(cloud: String, workspaceCloudMap: Map[String, String]): Boolean = {
    cloud.toLowerCase == "multi" && workspaceCloudMap.nonEmpty
  }

  private def verifyCloud(cloud: String): Unit = {
    cloud.toLowerCase match {
      case "azure" =>
      case "aws" =>
      case "gcp" =>
      case _ => throw new Exception(s"Supported Cloud Types are azure|aws|gcp")
    }
  }

  private def verifyCloudInputs(cloud: String, workspaceCloudMap: Map[String, String]): Unit = {

    if (cloud.toLowerCase == "multi" && workspaceCloudMap.isEmpty) {
      throw new Exception("ERROR: When specifying multi-cloud you must pass in a Map of org_id -> cloud")
    }

    if (isMultiCloud(cloud, workspaceCloudMap)) {
      workspaceCloudMap.values.map(_.toLowerCase).toArray.distinct.foreach(verifyCloud)
    } else {
      verifyCloud(cloud)
    }

  }

  def apply(
             spark: SparkSession,
             etlDB: String,
             cloud: String,
             workspaceCloudMap: Map[String, String] = Map[String, String](),
             tempDirPrefix: String = "/tmp/overwatch/instanceDetailsRefresh/"
           ): Unit = {

//    verifyETLDB
//    verifyPipelineReport Exists
//    verify all orgIds in multiCloud are present in PipelineReport
    verifyCloudInputs(cloud, workspaceCloudMap)


    // pass in lowerCase workspaceCloudMap

  }

}
