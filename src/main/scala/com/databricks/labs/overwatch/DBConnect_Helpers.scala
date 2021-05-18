package com.databricks.labs.overwatch

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.backend.common.rpc.CommandContext
import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.Initializer
import com.databricks.labs.overwatch.utils.{AuditLogConfig, AzureAuditLogEventhubConfig, DataTarget, DatabricksContractPrices, JsonUtils, OverwatchParams, TokenSecret}

import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.ForkJoinTaskSupport

trait DBConnect_Helpers {

  val storagePrefix = "/mnt/tomesdata/data/overwatch_global"
  val etlDB = "overwatch_etl"
  val consumerDB = "overwatch"
  val secretsScope = "aott-kv"
  val dbPATKey = "overwatch-pat"
  val ehName = "overwatch-evhub"
  val ehKey = "overwatch-eventhubs"
  val primordialDateString = "2021-05-01"
  val maxDaysToLoad = 30
  private val _scopes = "audit, sparkEvents, jobs, clusters, clusterEvents, notebooks, pools".split(", ")
//  val scopes = "audit, notebooks".split(", ")

  val parallelism = 16
  val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))

  protected def getWorkspace(scopes: Array[String] = _scopes): Workspace = {

    val dataTarget = DataTarget(
      Some(etlDB), Some(s"dbfs:/user/hive/warehouse/${etlDB}.db"), None,
      Some(consumerDB), Some(s"dbfs:/user/hive/warehouse/${consumerDB}.db")
    )

    val tokenSecret = TokenSecret(secretsScope, dbPATKey)
    val ehConnString = System.getenv("EHConnString") //dbutils.secrets.get(secretsScope, ehKey)

    val tempPath = "/tmp"
    val basePath = s"$tempPath/$etlDB"
    val badRecordsPath = s"$basePath/sparkEventsBadrecords"
    val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = ehConnString, eventHubName = ehName, auditRawEventsPrefix = basePath)
    val interactiveDBUPrice = 0.56
    val automatedDBUPrice = 0.26

    val params = OverwatchParams(
      auditLogConfig = AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig)),
      dataTarget = Some(dataTarget),
      tokenSecret = Some(tokenSecret),
      badRecordsPath = Some(badRecordsPath),
      overwatchScope = Some(scopes),
      maxDaysToLoad = maxDaysToLoad,
      databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice),
      primordialDateString = Some(primordialDateString)
    )

    val workspaceArgs = JsonUtils.objToJson(params).compactString
    if (workspaceArgs.nonEmpty) {
      Initializer(Array(workspaceArgs), debugFlag = true)
    } else {
      Initializer(Array())
    }
  }

}
