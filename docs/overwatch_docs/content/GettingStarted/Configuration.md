---
title: "Configuration"
date: 2020-10-28T11:55:12-04:00
draft: true
weight: 2
---

The configuration for Overwatch is fairly basic for the initial release but the plan is to open a lot of options
and make the config much more robust to allow for much more granular control over operations. The ongoing changes
will be documented here.

## Configuration Basics
The Overwatch configuration can be created as a case class of [OverwatchParams](#overwatchparams) or as a json string passed into
the main class `com.databricks.labs.overwatch.BatchRunner`. When passed in as a json string, it is 
deserialized into an instance of [OverwatchParams](#overwatchparams). This provides strong validation on the input parameters
and strong typing for additional validation options. All configs attempt to be defaulted to a reasonable default
where possible. If a default is set, passing in a value will simply overwrite the default.

## Config Examples
Below are some configurations examples of building the configuration parameters called *params* which is a variable
set to contain an instance of the class *OverwatchParams*.

Once the config has been created according to your needs, refer back to the 
[Getting Started - Run Via Notebook]({{%relref "GettingStarted"%}}#run-via-notebook) or
[Getting Started - Run Via Main Class]({{%relref "GettingStarted"%}}#run-via-main-class) section to see how it's can be used
when executing the Overwatch Run.

### Simplified Example
The simplest configuration possible. This is just for testing and likely is not a sufficient configuration for 
real-world application.

If using Azure you must change the auditLogConfig to the Azure version
which can be found below in [Azure Example](#azure-example)

```scala
import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver, Gold}
import com.databricks.labs.overwatch.utils._
private val db = "Overwatch_Test"
private val auditLogConfig = AuditLogConfig(Some("/path/to/aws/audit/logs"))
private val dataTarget = DataTarget(Some(db), Some(s"dbfs:/user/hive/warehouse/${db}.db"))
private val tokenSecret = TokenSecret("db_services", "databricks_overwatch")
private val overwatchModules = "audit,accounts,sparkEvents,jobs,clusters,clusterEvents,notebooks".split(",")

val params = OverwatchParams(auditLogConfig)
```

### Azure Example (Full)
Azure does require an event hub to be set up and referenced in the config as an 
[AzureAuditLogEventhubConfig](#azureauditlogeventhubconfig). More details for setting up this Event Hub
can be found in the [Azure Environment Setup]({{%relref "EnvironmentSetup/azure.md"%}}) page.
```scala
import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver, Gold}
import com.databricks.labs.overwatch.utils._
private val consumerDB = "Overwatch"
private val etlDB = "Overwatch_ETL"
private val evhName = "overwatch-evhub"
private val secretsScope = "aott-kv"
private val secretsEvHubKey = "overwatch-eventhubs"
private val overwatchKey = "overwatch-pat"
private val automatedDBUPrice = dbutils.widgets.get("automatedDBU")
private val interactiveDBUPrice = dbutils.widgets.get("interactiveDBU")

private val dataTarget = DataTarget(
  Some(etlDB), Some(s"dbfs:/user/hive/warehouse/$etlDB.db"),
  Some(consumerDB), Some(s"dbfs:/user/hive/warehouse/$consumerDB.db")
)
private val tokenSecret = TokenSecret(secretsScope, overwatchKey)
private val evhubConnString = dbutils.secrets.get(secretsScope, secretsEvHubKey)
private val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = evhubConnString, eventHubName = evhName, auditRawEventsPrefix = s"/tmp/$etlDB")
private val overwatchModules = "audit,accounts,sparkEvents,jobs,clusters,clusterEvents,notebooks".split(",")
private val databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice)

val params = OverwatchParams(
  auditLogConfig = AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig)),
  tokenSecret = Some(tokenSecret),
  dataTarget = Some(dataTarget),
  badRecordsPath = Some(s"/tmp/${etlDB}/sparkEventsBadrecords"),
  overwatchScope = Some(overwatchModules),
  maxDaysToLoad = 30,
  primordialDateString = Some("2021-01-01"),
  databricksContractPrices = databricksContractPrices
)
```

### AWS Example (Full)
```scala
import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver, Gold}
import com.databricks.labs.overwatch.utils._
private val consumerDB = "Overwatch"
private val etlDB = "Overwatch_ETL"
private val automatedDBUPrice = dbutils.widgets.get("automatedDBU")
private val interactiveDBUPrice = dbutils.widgets.get("interactiveDBU")

private val dataTarget = DataTarget(
  Some(etlDB), Some(s"dbfs:/user/hive/warehouse/$etlDB.db"),
  Some(consumerDB), Some(s"dbfs:/user/hive/warehouse/$consumerDB.db")
)
private val tokenSecret = TokenSecret("db_services", "databricks_overwatch")
private val overwatchModules = "audit,accounts,sparkEvents,jobs,clusters,clusterEvents,notebooks".split(",")
private val databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice)

val params = OverwatchParams(
  auditLogConfig = AuditLogConfig(Some("s3a://CORP_audit_logBucket/databricks/audit-logs")),
  tokenSecret = Some(tokenSecret),
  dataTarget = Some(dataTarget), 
  badRecordsPath = Some(s"/tmp/overwatch/sparkEventsBadrecords"),
  overwatchScope = Some(overwatchModules),
  maxDaysToLoad = 60,
  primordialDateString = Some("2021-01-01"),
  databricksContractPrices = databricksContractPrices
)
```

### OverwatchParams
The configuration structure required for Overwatch

Config | Required Override | Default Value | Type | Description
:--------------------------|:---|:----------|:----------|:--------------------------------------------------
**auditLogConfig**|Y|NULL|[AuditLogConfig](#auditlogconfig)|Databricks Audit Log delivery information.
**tokenSecret**|N|Execution Owner's Token|Option[\[TokenSecret\]](#tokensecret)|Secret retrieval information
**dataTarget**|N|DataTarget("overwatch", "/user/hive/warehouse/{databaseName}.db")|Option[\[DataTarget\]](#datatarget)|What to call the database and where to store it
**badRecordsPath**|N|/tmp/overwatch/badRecordsPath|Option\[String\]|When reading the log files, where should Overwatch store the records / files that cannot be parsed. Overwatch must have write permissions to this path 
**overwatchScope**|N|all|Option\[Seq\[String\]\]|List of [modules]({{%relref "GettingStarted/Modules.md"%}}) in scope for the run. It's important to note that there are many co-dependencies. When choosing a module, be sure to also enable it's requisites. If not value provided, all modules will execute.
**maxDaysToLoad**|N|60|Int|On large, busy workspaces 60 days of data may amount in 10s of TB of raw data. This parameter allows the job to be broken out into several smaller runs.
**primordialDateString**|N|Today's date minus 60 days, format = "yyyy-mm-dd"|String|Date from which data collection was to begin. This is the earliest date for which data should attempted to be collected.
**databricksContractPrices**|N|DatabricksContractPrices(0.56, 0.26)|[DatabricksContractPrices](#databrickscontractprices)|Allows the user to globally configure Databricks contract prices to improve dollar cost estimates where referenced. Additionally, these values will be added to the *instanceDetails* consumer table for custom use. They are also available in com.databricks.labs.overwatch.utils.DBContractPrices(). 

### AuditLogConfig
Config to point Overwatch to the location of the delivered audit logs

Config | Required Override | Default Value | Type | Description
:--------------------------|:---|:----------|:----------|:--------------------------------------------------
**rawAuditPath**|Y|None|Option[String]|Top-level path to directory containing workspace audit logs delivered by Databricks. The Overwatch user must have read access to this path
**azureAuditLogEventhubConfig**|Y (only on Azure)|None|Option[AzureAuditLogEventhubConfig](#azureauditlogeventhubconfig)|Required configuration when using Azure as Azure must deliver audit logs via LogAnalytics via Eventhub

### TokenSecret
Overwatch must have permission to perform its functions; these are further discussed in [AdvancedTopics](advancedtopics.md).
The token secret stores the Databricks Secret scope / key for Overwatch to retrieve. The key should store the 
token secret to be used which usually starts with "dapi..." 

If no TokenSecret is passed into the config, the operation owner's token will be used. If Overwatch is being 
executed in a notebook the notebook user's token will be used. If Overwatch is being executed through a job the 
token of the job owner will be used. Whatever token is used, it must have the appropriate access or it will result 
in missing data, or an Overwatch run failure.

Config | Required Override | Default Value | Type | Description
:--------------------------|:---|:----------|:----------|:--------------------------------------------------
**scope**|N|NA|String|Databricks secret scope 
**key**|N|NA|String|Databricks secret key within the scope defined in the Scope parameter

### DataTarget
Where to create the database and what to call it. This must be defined on first run or Overwatch will create a
database named "Overwatch" at the default location which is "/user/hive/warehouse/overwatch.db". This is challenging
to change later, so be sure you choose a good starting point. After the initial run, this must not change without 
and entire [database migration](AdvancedTopics.md). Overwatch performs destructive tasks within its own database and
this is how it protects itself against harming existing data. Overwatch creates specific metadata inside the database
at creation time to ensure the database is created and owned by the Overwatch process. Furthermore, metadata is 
managed to track schema versions and other states.

Config | Required Override | Default Value | Type | Description
:--------------------------|:---|:-----|:----------|:--------------------------------------------------
**databaseName**|N|Overwatch|Option[String]|Name of the primary database to be created on first run or to which will be appended on subsequent runs. This database is typically used as the ETL database only as the consumer database is also usually specified to have a different name. If consumerDatabase is also specified in the configuration, on the ETL entities will be stored in this datbase.
**databaseLocation**|N|/user/hive/warehouse/{databaseName}.db|Option[String]|Location of the Overwatch database. Any compatible fully-qualified URI can be used here as long as Overwatch has access to write the target. Most customers, however, mount the qualified path and reference the mount point for simplicity but this is not required and may not be possible depending on security requirements and environment.
**consumerDatabaseName**|N|{databaseName}|Option[String]|Will be the same as the databaseName if not otherwise specified. Holds the user-facing entities and separates them from all the intermediate ETL entities for a less cluttered experience, easy-to-find entities, and simplified security.
**ConsumerDatabaseLocation**|N|/user/hive/warehouse/{consumerDatabaseName}.db|Option[String]|*See databaseLocation above*

### AzureAuditLogEventhubConfig
Not Required when using AWS <br>
Eventhub streaming environment configurations 

Config | Required Override | Default Value | Type | Description
:--------------------------|:---|:----------|:----------|:--------------------------------------------------
**connectionString**|Y|NA|String|Retrieve from Azure Portal Event Hub
**eventHubName**|Y|NA|String|Retrieve from Azure Portal Event Hub
**auditRawEventsPrefix**|Y|NA|String|Path prefix for checkpoint directories
**maxEventsPerTrigger**|N|10000|Int|Events to pull for each trigger, this should be increased during initial cold runs or runs that have very large numbers of audit log events.
**auditRawEventsChk**|N|{auditRawEventsPrefix}/rawEventsCheckpoint|Option[String]|Checkpoint Directory name for the raw dump of events from Eventhub. This directory gets overwritten upon successful pull into Overwatch.
**auditLogChk**|N|{auditRawEventsPrefix}/auditLogBronzeCheckpoint|Option[String]|**DEPRECATED** Checkpoint Directory name for the audit log stream target. This target will continuously grow as more audit logs are created and delivered

### DatabricksContractPrices
Config | Required Override | Default Value | Type | Description
:--------------------------|:---|:----------|:----------|:--------------------------------------------------
**interactiveDBUCostUSD**|N|0.56|Double|Approximate list price of interactive DBU
**automatedDBUCostUSD**|N|0.23|Double|Approximate list price of automated DBU
