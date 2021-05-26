---
title: "Configuration"
date: 2020-10-28T11:55:12-04:00
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

{{% notice warning%}}
The configuration examples below use a data target prefix of */user/hive/warehouse*. It is strongly recommended that
a data target with a prefix other than /user/hive/warehouse be used. Even though the tables are external, any 
spark database located within the default warehouse will drop all tables (including external) with certain drop database
commands resulting in permanent data loss. This is default behavior and has nothing to do with Overwatch specifically
but it's called out here as a major precaution. If multiple workspaces are to be configured and someone with write 
access to this location invokes certain drop database commands, all tables will be permanently deleted for all 
workspaces.
{{% /notice %}}

### Simplified Example
The simplest configuration possible. This is just for testing and likely is not a sufficient configuration for 
real-world application.

If using Azure you must change the auditLogConfig to the Azure version
which can be found below in [Azure Example](#azure-example)

```scala
import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver, Gold}
import com.databricks.labs.overwatch.utils._
private val storagePrefix = "/mnt/overwatch/"
private val db = "Overwatch_Test"
private val auditLogConfig = AuditLogConfig(Some(s"${storagePrefix}/workspace_id/audit_logs"))
private val dataTarget = DataTarget(Some(db), Some(s"${storagePrefix}/workspace_id/${db}.db"), Some(s"${storagePrefix}/global_data"))
private val tokenSecret = TokenSecret("db_services", "databricks_overwatch")
private val overwatchModules = "audit,accounts,sparkEvents,jobs,clusters,clusterEvents,notebooks".split(",")

val params = OverwatchParams(auditLogConfig)
```

### Azure Example (Full)
Azure does require an event hub to be set up and referenced in the config as an 
[AzureAuditLogEventhubConfig](#azureauditlogeventhubconfig). More details for setting up this Event Hub
can be found in the [Azure Environment Setup]({{%relref "EnvironmentSetup/azure.md"%}}) page. An example notebook
can be found [here](/assets/GettingStarted/azure_runner_docs_example.html).
```scala
import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver, Gold}
import com.databricks.labs.overwatch.utils._
private val dataTarget = DataTarget(
  Some(etlDB), Some(s"${storagePrefix}/${workspaceID}/${etlDB}.db"), Some(s"${storagePrefix}/global_share"),
  Some(consumerDB), Some(s"${storagePrefix}/${workspaceID}/${consumerDB}.db")
)

private val tokenSecret = TokenSecret(secretsScope, dbPATKey)
private val ehConnString = dbutils.secrets.get(secretsScope, ehKey)

private val ehStatePath = s"${storagePrefix}/${workspaceID}/ehState"
private val badRecordsPath = s"${storagePrefix}/${workspaceID}/sparkEventsBadrecords"
private val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = ehConnString, eventHubName = ehName, auditRawEventsPrefix = ehStatePath)
private val interactiveDBUPrice = 0.56
private val automatedDBUPrice = 0.26

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
```

**NOTE** The connection string stored in the *ehConnString* above is stored as a secret since it contains a key. 
**THIS IS NOT THE KEY** but the actual connection string from the SAS Policy. To find this follow the path in the 
Azure portal below.

eh-namespace --> eventhub --> shared access policies --> Connection String-primary key

The connection string should begin with `Endpoing=sb://`. Note that the policy only needs the Listen permission

![ConnStringExample](/images/GettingStarted/Azure_EH_ConnString.png)

### AWS Example (Full)
An example notebook can be found [here](/assets/GettingStarted/aws_runner_docs_example.html).
```scala
import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver, Gold}
import com.databricks.labs.overwatch.utils._
private val dataTarget = DataTarget(
  Some(etlDB), Some(s"${storagePrefix}/${workspaceID}/${etlDB}.db"), Some(s"${storagePrefix}/global_share"),
  Some(consumerDB), Some(s"${storagePrefix}/${workspaceID}/${consumerDB}.db")
)

private val tokenSecret = TokenSecret(secretsScope, dbPATKey)
private val badRecordsPath = s"${storagePrefix}/${workspaceID}/sparkEventsBadrecords"
private val auditSourcePath = s"${storagePrefix}/${workspaceID}/raw_audit_logs"
private val interactiveDBUPrice = 0.56
private val automatedDBUPrice = 0.26

val params = OverwatchParams(
  auditLogConfig = AuditLogConfig(rawAuditPath = Some(auditSourcePath)),
  dataTarget = Some(dataTarget),
  tokenSecret = Some(tokenSecret),
  badRecordsPath = Some(badRecordsPath),
  overwatchScope = Some(scopes),
  maxDaysToLoad = maxDaysToLoad,
  databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice),
  primordialDateString = Some(primordialDateString)
)
```

### OverwatchParams
The configuration structure required for Overwatch

Config | Required Override | Default Value | Type | Description
:--------------------------|:---|:----------|:----------|:--------------------------------------------------
**auditLogConfig**|Y|NULL|[AuditLogConfig](#auditlogconfig)|Databricks Audit Log delivery information.
**tokenSecret**|N|Execution Owner's Token|Option[\[TokenSecret\]](#tokensecret)|Secret retrieval information
**dataTarget**|N|DataTarget("overwatch", "/user/hive/warehouse/{databaseName}.db", "/user/hive/warehouse/{databaseName}.db")|Option[\[DataTarget\]](#datatarget)|What to call the database and where to store it
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
Overwatch must have permission to perform its functions; these are further discussed in [AdvancedTopics]({{%relref "GettingStarted/advancedtopics.md"%}}).
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
and entire [database migration]({{%relref "GettingStarted/advancedtopics.md"%}}). Overwatch can perform destructive tasks within its own database and
this is how it protects itself against harming existing data. Overwatch creates specific metadata inside the database
at creation time to ensure the database is created and owned by the Overwatch process. Furthermore, metadata is 
managed to track schema versions and other states.

Config | Required Override | Default Value | Type | Description
:--------------------------|:---|:-----|:----------|:--------------------------------------------------
**databaseName**|N|Overwatch|Option[String]|Name of the primary database to be created on first run or to which will be appended on subsequent runs. This database is typically used as the ETL database only as the consumer database is also usually specified to have a different name. If consumerDatabase is also specified in the configuration, on the ETL entities will be stored in this datbase.
**databaseLocation**|N|/user/hive/warehouse/{databaseName}.db|Option[String]|Location of the Overwatch database. Any compatible fully-qualified URI can be used here as long as Overwatch has access to write the target. Most customers, however, mount the qualified path and reference the mount point for simplicity but this is not required and may not be possible depending on security requirements and environment.
**etlDataPathPrefix**|N|{databaseLocation}|Option[String]|The location the data will actually be stored. This is critical as data (even EXTERNAL) stored underneath a database path can be deleted if a user call drop database or drop database cascade. This is even more significant when working with multiple workspaces as the risk increases with the breadth of access. 
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
**automatedDBUCostUSD**|N|0.26|Double|Approximate list price of automated DBU
