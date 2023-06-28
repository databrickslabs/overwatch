package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.pipeline.Pipeline
import io.delta.tables.DeltaTable
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

private class RefreshNodeDetails(
                                  spark: SparkSession,
                                  etlDB: String,
                                  cloud: String,
                                  workspaceCloudMap: Map[String, String], // orgId -> cloud
                                  asOfDate: String, // yyyy-mm-dd
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

  private val instanceDetailsDF: DataFrame = spark.table(s"$etlDB.instanceDetails")

  // organization_ids in scope
  private val scopedOrgIds: Array[String] = if (isMultiCloud) {
    workspaceCloudMap.keys.toArray
  } else rawPipReportDF.select('organization_id).distinct.as[String].collect()

  // organization_ids by cloud
  private val orgIdsByCloud: Map[String, Array[String]] = if (isMultiCloud) {
    workspaceCloudMap.groupBy(_._2).flatMap(y => Map(y._1 -> y._2.keys.toArray))
  } else Map[String, Array[String]](cloud -> scopedOrgIds)

  private val scopedOrgIdsFilter: Column = col("organization_id").isin(scopedOrgIds: _*)

  private val scopedPipReportDF = rawPipReportDF
    .filter(scopedOrgIdsFilter)


  private val primordialDateW = Window.partitionBy('organization_id).orderBy('Pipeline_SnapTS.desc)
  private val originalMetaDF = scopedPipReportDF
    .withColumn("rnk", rank().over(primordialDateW))
    .withColumn("rn", row_number().over(primordialDateW))
    .filter('rnk === 1 && 'rn === 1)
    .select('organization_id, 'primordialDateString, 'workspace_name)
    .distinct

  private val keyFields = Array("organization_id", "API_Name")
  private val mergeCondition =
    """
  updates.mergeK1 = target.organization_id AND
  updates.mergeK2 = target.API_Name
  """

  // ensure that all workspaceIDs passed in the workspaceCloudMap actually have executed a run and can be found
  // in the pipReport
  private def validateMultiWorkspaceIds: this.type = {

    val pipReportOrgIds = scopedPipReportDF.select('organization_id).distinct()
      .as[String]
      .collect()
      .toSet

    val scopedLessPipReport = scopedOrgIds.toSet -- pipReportOrgIds
    assert(scopedLessPipReport.isEmpty,
      s"""
         |Workspace IDs found in workspaceCloudMap that are not present in the pipeline_report table.
         |Ensure that at least one run for all workspaces has completed before using this function.
         |Missing workspaceIDs include:\n${scopedLessPipReport.mkString("\n")}
         |""".stripMargin)

    this
  }

  // Drop any records that have a null for hourly dbus
  // to ensure healthy value for each record
  private def dropNullHourlyDBURecords: this.type = {
    spark.sql(s"delete from $etlDB.instanceDetails where Hourly_DBUs is null")

    this
  }

  // define the location of the main git resource file by cloud
  private def getStaticCloudResourceLink(cloud: String): String = {
    cloud.toLowerCase match {
      case "azure" => "https://raw.githubusercontent.com/databrickslabs/overwatch/main/src/main/resources/Azure_Instance_Details.csv"
      case "aws" => "https://raw.githubusercontent.com/databrickslabs/overwatch/main/src/main/resources/AWS_Instance_Details.csv"
      case "gcp" => "https://raw.githubusercontent.com/databrickslabs/overwatch/main/src/main/resources/Gcp_Instance_Details.csv"
      case _ => throw new Exception(s"Supported Cloud Types are azure|aws|gcp")
    }
  }

  // name the local temp files by cloud
  private def getLocalStaticResourceFileName(cloud: String): String = {
    cloud.toLowerCase match {
      case "azure" => "Azure_Instance_Details.csv"
      case "aws" => "AWS_Instance_Details.csv"
      case "gcp" => "Gcp_Instance_Details.csv"
      case _ => throw new Exception(s"Supported Cloud Types are azure|aws|gcp")
    }
  }

  // land the raw files from github main branch to the temp dir
  private def landStaticResourceFiles: this.type = {
    distinctClouds.foreach(cloudString => {
      val url = getStaticCloudResourceLink(cloudString)
      sc.addFile(url)
      val fileName = getLocalStaticResourceFileName(cloudString)
      val staticResourcePath = SparkFiles.get(fileName)
      dbutils.fs.cp(s"file://$staticResourcePath", s"$tempWorkingDir/$fileName")
    })
    this
  }

  // build the dataframe for all clouds in scope from the git hub main branch
  private def getLatestInstanceDetailsDF: DataFrame = {

    val subsequentClouds = distinctClouds.drop(1)
    val firstCloud = distinctClouds(0)

    // build df for first cloud in scope
    val firstCloudInstanceDetailsDF = spark.read.option("header", "true").format("csv")
      .load(s"$tempWorkingDir/${getLocalStaticResourceFileName(firstCloud)}")
      .withColumn("organization_id", explode(lit(orgIdsByCloud(firstCloud))))

    // union all subsequent clouds to the first df using foldLeft
    val rawUpdatesDF = subsequentClouds.foldLeft(firstCloudInstanceDetailsDF)((df, cloudString) => {
      val filePathX = s"$tempWorkingDir/${getLocalStaticResourceFileName(cloudString)}"
      val dfX = spark.read.option("header", "true").format("csv")
        .load(filePathX)
        .withColumn("organization_id", explode(lit(orgIdsByCloud(cloudString))))
      df.unionByName(dfX, allowMissingColumns = true)
    })

    // add additional meta df such as primordial_date and workspace_name
    rawUpdatesDF
      .join(originalMetaDF, Seq("organization_id"))

  }

  // build the updates dataframe to be merged into instanceDetails
  private def deriveMergeDF: DataFrame = {

    val activeAsOfDate = if (asOfDate == "") current_date() else lit(asOfDate).cast("date")

    val fieldsToUpdate = getLatestInstanceDetailsDF.columns
      .filterNot(fieldName => keyFields.map(_.toLowerCase).contains(fieldName.toLowerCase))

    // append _update to all fields except the key fields
    val latestInstanceDetailsFromGitDF = fieldsToUpdate
      .foldLeft(getLatestInstanceDetailsDF)((df, f) =>
        df.withColumnRenamed(f, s"${f}_update")
      )

    // full join to get original df with all values from git where git fields (less keys) are renamed to be
    // suffixed with _update
    val mergeUpdatesDF = instanceDetailsDF
      .join(latestInstanceDetailsFromGitDF, keyFields, "full")

    // identify record status as determined by whether it's a new key, changed value, or no change
    val isUpdateLogic = 'Hourly_DBUs =!= 'Hourly_DBUs_update && coalesce('Hourly_DBUs, 'Hourly_DBUs_update).isNotNull
    val recordStatus = when(isUpdateLogic, array(lit("update"), lit("newActive")))
      .when('Hourly_DBUs.isNull, array(lit("insert")))
      .otherwise(array(lit("noUpdate")))

    // define merge key logic for first key
    val mergeK1Logic = when('rowStatus === "update", col(keyFields(0)))
      .otherwise(lit(null).cast("string"))

    // define merge key logic for second key
    val mergeK2Logic = when('rowStatus === "update", col(keyFields(1)))
      .otherwise(lit(null).cast("string"))

    // newActiveRecords should get activeFrom expiryDate
    // insertRecords should be activeFrom primordial date
    // all other records (updates) should keep their original activeFrom
    val activeFromLogic = when('rowStatus === "newActive", activeAsOfDate)
      .when('rowStatus === "insert", 'primordialDateString_update)
      .otherwise('activeFrom)

    // when record is to be expired (i.e. isUpdate) set activeUntil = activeAsOfDate
    // otherwise the record should be active; thus it should be null
    val updateActiveUntilLogic = when('rowStatus === "update", activeAsOfDate)
      .otherwise(lit(null).cast("date"))

    // updates are actually to be expired records and should no longer be active
    val updateIsActiveLogic = when('rowStatus === "update", lit(false))
      .otherwise(lit(true))

    // expire original records that need an update, don't change the value
    // inserts and newActives should get the new value
    val valueFinal = when('rowStatus =!= "update", 'Hourly_DBUs_update) // updates just expire don't change value
      .otherwise('Hourly_DBUs)

    mergeUpdatesDF // full joined dataset
      .withColumn("rowStatus", explode(recordStatus)) // get one record for each type of update / insert
      .filter('rowStatus =!= "noUpdate") // remove all records with no changes
      .withColumn("mergeK1", mergeK1Logic) // define merge key 1
      .withColumn("mergeK2", mergeK2Logic) // define merge key 2
      .withColumn("Hourly_DBUs", valueFinal) // set value to the correct value
      .withColumn("activeFrom", activeFromLogic) // set activeFrom to correct value
      .withColumn("activeUntil", updateActiveUntilLogic) // set activeUntil to correct value
      .withColumn("isActive", updateIsActiveLogic) //set isActive to correct value
      .withColumn("Pipeline_SnapTS", refreshSnapTime.asColumnTS)
      .withColumn("Overwatch_RunID", lit("explicit_update"))

  }

  // execute the actual merge to upsert to instanceDetails
  private def executeMerge: this.type = {

    val deltaDataTable = DeltaTable.forName(s"$etlDB.instanceDetails")
    val exclusionFields = keyFields ++ Array("Hourly_DBUs", "activeFrom", "activeUntil", "isActive", "Pipeline_SnapTS", "Overwatch_RunID")
    val insertValueFieldMappings = deltaDataTable.toDF.columns
      .filterNot(fieldName => exclusionFields.map(_.toLowerCase).contains(fieldName.toLowerCase))
      .map(c => (s"target.$c", s"updates.${c}_update")).toMap
    val insertManualFieldMapping = exclusionFields
      .map(c => (s"target.$c", s"updates.$c")).toMap
    val insertFieldMappings = insertManualFieldMapping ++ insertValueFieldMappings

    deltaDataTable.alias("target")
      .merge(deriveMergeDF.alias("updates"), mergeCondition)
      .whenMatched()
      .updateExpr(Map(
        "target.isActive" -> "false",
        "target.activeUntil" -> s"cast('$asOfDate' as date)"
      ))
      .whenNotMatched()
      .insertExpr(insertFieldMappings)
      .execute()
    this
  }

  private def cleanup(): Unit = {
    dbutils.fs.rm(tempWorkingDirPrefix, recurse = true)
  }


}

object RefreshNodeDetails {

  private def isMultiCloud(cloud: String, workspaceCloudMap: Map[String, String]): Boolean = {
    cloud.toLowerCase == "multi" && workspaceCloudMap.nonEmpty
  }

  // ensure the etlDB exists and contains the required tables
  private def verifyETLDB(spark: SparkSession, etlDB: String): Unit = {
    val requiredTables = Array("instanceDetails", "pipeline_report")
    requiredTables.foreach(table => {
      try {
        spark.table(s"$etlDB.$table")
      } catch {
        case e: AnalysisException =>
          println(s"REQUIRED TABLE $etlDB.$table NOT FOUND")
          throw e
      }
    })
  }

  // valid clouds
  private def verifyCloud(cloud: String): Unit = {
    cloud.toLowerCase match {
      case "azure" =>
      case "aws" =>
      case "gcp" =>
      case _ => throw new Exception(s"Supported Cloud Types are azure|aws|gcp")
    }
  }

  // if customer is multi-cloud, they must specify "multi" as the cloud and provide a workspace_id -> cloud
  // map -- if they do not follow this, fail.
  private def verifyCloudInputs(cloud: String, workspaceCloudMap: Map[String, String]): Unit = {

    if (cloud.toLowerCase == "multi" && workspaceCloudMap.isEmpty) {
      throw new Exception("ERROR: When specifying multi-cloud you must pass in a Map of org_id -> cloud")
    }

    if (isMultiCloud(cloud, workspaceCloudMap)) {
      workspaceCloudMap.values.map(_.toLowerCase).toArray.distinct.foreach(verifyCloud)
      assert(workspaceCloudMap.keys.toArray.length == workspaceCloudMap.keys.toArray.distinct.length,
        """
          |One or more workspaceIDs are specified in the workspace cloud map more than once
          |""".stripMargin)
    } else {
      verifyCloud(cloud)
    }

  }

  /**
   *
   * @param spark spark session
   * @param etlDB Overwatch ETL Database
   * @param cloud Cloud Provider -- One of aws, azure, gcp, multi
   * @param workspaceCloudMap when cloud == multi a map of workspace_id -> cloud must be provided
   *                          such as Map("0123" -> "azure", "4567" -> "aws")
   * @param asOfDate defaults to current date. Format must be yyyy-mm-dd Specifies the date at which previously
   *                 records will be expired and newly active records will become active.
   *                 Used to specify updates retroactively
   * @param tempDirPrefix defaults to /tmp/overwatch/instanceDetailsRefresh/ but may be overridden if this temp dir
   *                      is not sufficient for the user.
   */
  def executeUpdate(
                     spark: SparkSession,
                     etlDB: String,
                     cloud: String,
                     workspaceCloudMap: Map[String, String] = Map[String, String](),
                     asOfDate: String = "",
                     tempDirPrefix: String = "/tmp/overwatch/instanceDetailsRefresh/"
                   ): Unit = {

    // TODO -- 904 - add minScheam for instanceDetails and dbuCostDetails
    // TODO -- lowercase / trim api_name for merge condition?
    verifyETLDB(spark, etlDB)
    verifyCloudInputs(cloud, workspaceCloudMap)

    new RefreshNodeDetails(
      spark,
      etlDB,
      cloud,
      workspaceCloudMap.mapValues(_.toLowerCase),
      asOfDate,
      tempDirPrefix
    ).validateMultiWorkspaceIds
      .dropNullHourlyDBURecords
      .landStaticResourceFiles
      .executeMerge
      .cleanup()
  }

}
