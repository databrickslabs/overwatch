// Databricks notebook source
dbutils.widgets.text("dbName", "", "Overwatch database name")
dbutils.widgets.text("evhName", "", "Name of the EventHubs topic with diagnostic data")
dbutils.widgets.text("secretsScope", "", "Name of the secret scope")
dbutils.widgets.text("secretsEvHubKey", "", "Secret key name for EventHubs connection string")
dbutils.widgets.text("overwatchDBKey", "", "Secret key name for DB PAT (personal access token)")
dbutils.widgets.text("tempPath", "/tmp/overwatch", "Path to store broken records, checkpoints, etc.")

// COMMAND ----------

val dbName = dbutils.widgets.get("dbName")
val evhName = dbutils.widgets.get("evhName")
val secretsScope = dbutils.widgets.get("secretsScope") // "aott-kv-scope"
val secretsEvHubKey = dbutils.widgets.get("secretsEvHubKey") // "overwatch-eventhubs"
val overwatchDBKey = dbutils.widgets.get("overwatchDBKey") // "overwatch-pat"
val tempPath = dbutils.widgets.get("tempPath") // 

if (dbName.isEmpty || evhName.isEmpty || secretsScope.isEmpty || secretsEvHubKey.isEmpty || overwatchDBKey.isEmpty) {
  throw new IllegalArgumentException("Please specify all required parameters!")
}

// COMMAND ----------

import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver}
import com.databricks.labs.overwatch.utils._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

private val dataTarget = DataTarget(Some(dbName), Some(s"dbfs:/user/hive/warehouse/$dbName.db"))

private val tokenSecret = TokenSecret(secretsScope, overwatchDBKey)
val evhubConnString = dbutils.secrets.get(secretsScope, secretsEvHubKey)

val basePath = s"$tempPath/$dbName"
val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = evhubConnString, eventHubName = evhName, auditRawEventsPrefix = basePath)

val params = OverwatchParams(
  auditLogConfig = AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig)),
  dataTarget = Some(dataTarget), 
  tokenSecret = Some(tokenSecret),
  badRecordsPath = Some(s"$basePath/sparkEventsBadrecords"),
  overwatchScope = Some("audit,accounts,jobs,sparkEvents,clusters,clusterEvents,notebooks".split(",")),
  maxDaysToLoad = 60
)

private val args = JsonUtils.objToJson(params).compactString
val workspace = if (args.length != 0) {
  Initializer(Array(args), debugFlag = true)
} else { 
  Initializer(Array()) 
}

// COMMAND ----------

Bronze(workspace).run()

// COMMAND ----------

Silver(workspace).run()

// COMMAND ----------


