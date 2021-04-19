---
title: "Getting Started"
date: 2020-10-27T16:40:00-04:00
draft: true
weight: 1
---

## Run Methods
Overwatch is usually run via one of two methods:
1. As a job controlling [**running a notebook**](#run-via-notebook)
2. As a job running the [**main class**](#run-via-main-class) with the configs

Below are instructions for both methods. The primary difference is the way by which the
[configuration]({{%relref "GettingStarted/Configuration.md"%}}) is implemented and the choice of which to use is
entirely user preference. Overwatch is meant to control itself from soup to nuts meaning it creates its own databases
and tables and manages all spark parameters and optimization requirements to operate efficiently. Beyond the config
and the job run setup, Overwatch runs best as a black box -- enable it and forget about it.

## Dependencies
Add the following dependencies to your cluster
* plotly -- pretty reporting for some reports, whether or not you will need this depends on the reports you wish
  to run
    * Default PyPi - Tested with version 4.8.2
* (only for Azure deployment) azure-eventhubs-spark - integration with Azure EventHubs
    * Maven Coordinate: `~com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18`

### Environment Setup
There are some basic environment configuration steps that need to be enabled to allow Overwatch to do its job.

These activities include things like [enabling Audit Logs](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html), setting up security (as desired), etc. The setup activities
are slightly different depending on your cloud provider, currently Overwatch can operate on
[**aws**]({{%relref "EnvironmentSetup/AWS.md"%}}) and [**azure**]({{%relref "EnvironmentSetup/Azure.md"%}}). Please
reference the page that's right for you.

### Cluster Requirements
* DBR 7.6
* Add the relevant [dependencies](#dependencies)
* Azure - Spot Instances not yet tested
  
#### Notes on Autoscaling
* Auto-scaling compute -- **Not Recommended** 
  Note that autoscaling compute will not be extremely efficient due to some of the compute tails 
  as a result of log file size skew and storage mediums. Additionally, some modules require thousands of API calls 
  (for historical loads) and have to be throttled to protect the workspace. As such, it's recommended to not use 
  autoscaling at this time, but rather use a smaller cluster and just let Overwatch run. Until [Issue 16](https://github.com/databrickslabs/overwatch/issues/16)
  can be implemented there will be a few inefficiencies.
* Auto-scaling Storage -- **Strongly Recommended**
    * Some of the data sources can grow to be quite large and require very large shuffle stages which requires
      sufficient local disk. If you choose not to use auto-scaling storage be sure you provisions sufficient local
      disk space.
    * SSD or NVME (preferred) -- It's strongly recommended to use fast local disks as there can be very large shuffles

### Run via Notebook
Example Notebook ([HTML](/assets/GettingStarted/Runner_Job.html) / [DBC](/assets/GettingStarted/Runner_Job.dbc)) --
Create a notebook like the one referenced and substitute your
[**configuration**]({{%relref "GettingStarted/Configuration.md"%}}) overrides.

* [Setup The Environment](#environment-setup) for your cloud provider
* Create/Define cluster commensurate with the [Cluster Requirements](#cluster-requirements)
* Notebook Basic Flow
    * Imports
    * Define Global Variables / Parameters (as needed)
    * Build the Config, the config is implemented through ["OverwatchParams"](TODO-LINKTO github class) case class
    * Initialize the Environment
    * Run Bronze --> Silver --> Gold Pipelines
```scala
val db = dbutils.widgets.get("dbName")
val etlDBName = dbutils.widgets.get("ETLDBName")
val automatedDBUPrice = dbutils.widgets.get("automatedDBU")
val interactiveDBUPrice = dbutils.widgets.get("interactiveDBU")

import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver, Gold}
import com.databricks.labs.overwatch.utils._

private val dataTarget = DataTarget(
  Some(etlDBName), Some(s"dbfs:/user/hive/warehouse/$etlDBName.db"),
  Some(db), Some(s"dbfs:/user/hive/warehouse/$db.db")
)
private val tokenSecret = TokenSecret("databricks", "overwatch_key")
private val databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice)

val params = OverwatchParams(
  auditLogConfig = AuditLogConfig(Some("s3a://auditLogBucketMount/audit-logs")),
  tokenSecret = Some(tokenSecret),
  dataTarget = Some(dataTarget), 
  badRecordsPath = Some(s"/tmp/tomes/overwatch/sparkEventsBadrecords"),
  overwatchScope = Some("audit,accounts,sparkEvents,jobs,clusters,clusterEvents,notebooks".split(",")),
  maxDaysToLoad = 30,
  databricksContractPrices = databricksContractPrices
)

private val args = JsonUtils.objToJson(params).compactString
val workspace = Initializer(Array(args), debugFlag = false)

Bronze(workspace).run()
Silver(workspace).run()
Gold(workspace).run()
```


### Run via Main Class
The Main class version does the same thing as the notebook version the difference is just how it's kicked off and
configured. The tricky party about the main class is that the parameters must be passed in as a string argument in
string-escaped json. The "OverwatchParams" object gets jsonified and passed into the args of the Main Class. To
simplify the creation of this string you can simply run the the exact same code as the
[Notebook](#run-via-notebook) runner **EXCEPT** don't run the Pipeline Runners (i.e. Bronze(workspace).run()).
Instead after the OverwatchParams class is instantiated, use the JsonUtils to output an escaped string.
```scala
val params = ...
JsonUtils.objToJson(params).escapedString
``` 
This will output the string needed to be placed in the Main Class args. <br>
Now that you have the string, setup the job. The Main Class for histoical batch is
`com.databricks.labs.overwatch.BatchRunner`<br>

![job_params](/images/GettingStarted/job_params.png) <br>
![jarSetupExample](/images/GettingStarted/jarSetupExample.png)

#### The new Jobs UI
There's a new jobs UI being rolled out around the same time as Overwatch. Furthermore, note that Overwatch will be 
accessible via Maven Central and it can be referenced that way as well.

![newUIJarSetup](/images/GettingStarted/jarSetupNewUI.png) <br>
![newUIJarSetup](/images/GettingStarted/OverwatchViaMaven.png)

### Modules
Choosing which modules are right for your organization can seem a bit overwhelming, but for now it should be
quite simple as several modules aren't yet released in this version. "Modules" are also sometimes
referred to as "Scope", know that the meaning is synonymous. To understand which modules are available in your
version and get more details on what's included, please refer to [Modules]({{%relref "GettingStarted/Modules.md"%}})

### Security Considerations
Overwatch, by default, will create a single database that, is accessible to everyone in your organization unless you
specify a location for the database in the configuration that is secured at the storage level. Several of the modules
capture fairly sensitive data such as users, userIDs, etc. It is suggested that the configuration specify two
databases in the configuration:
* **ETL database** -- hold all of the raw and intermediate transform entities. This database can be secured
  allowing only the necessary data engineers direct access.
* **Consumer database** -- Holds views only and is easy to secure using Databricks' table ACLs (assuming no direct
  scala access). The consumer database holds only views that point to tables so additional security can easily be
  attributed at this layer.

For more information on how to configure the separation of ETL and consumption databases, please reference the
[**configuration**]({{%relref "GettingStarted/Configuration.md"%}}) page.

Additional steps can be taken to secure the storage location of the ETL entities as necessary. The method for
securing access to these tables would be the same as with any set of tables in your organization.
