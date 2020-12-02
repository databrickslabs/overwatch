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
[configuration]({{%relref "UserGuide/Configuration.md"%}}) is implemented and the choice of which to use is
entirely user preference. Overwatch is meant to control itself from soup to nuts meaning it creates its own databases
and tables and manages all spark parameters and optimization requirements to operate efficiently. Beyond the config
and the job run setup, Overwatch runs best as a black box -- enable it and forget about it.

## Dependencies
Add the following dependencies to your cluster
* plotly -- pretty reporting for some reports, whether or not you will need this depends on the reports you wish 
to run
    * Default PyPi - Tested with version 4.8.2
* (only for Azure deployment) azure-eventhubs-spark - integration with Azure EventHubs
    * Maven Coordinate: `~com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.17`
    
### Environment Setup
There are some basic environment configuration steps that need to be enabled to allow Overwatch to do its job. 

These activities include things like [enabling Audit Logs](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html), setting up security (as desired), etc. The setup activities
are slightly different depending on your cloud provider, currently Overwatch can operate on 
[**aws**]({{%relref "EnvironmentSetup/AWS.md"%}}) and [**azure**]({{%relref "EnvironmentSetup/Azure.md"%}}). Please 
reference the page that's right for you.

### Cluster Requirements
* DBR 6.4 - DBR 6.6
    * Spark 3 (DBR 7.x) has not yet been tested. Spark 3 implementation will be coming soon
* Add the relevant [dependencies](#dependencies)
* Auto-scaling compute (Strongly Recommended)
    * Some of the modules paginate through API results which can take quite a bit of time and don't need a large 
    number of worker nodes to progress whereas other modules require complex compute and having a larger cluster
    will greatly speed up the job
* Auto-scaling Storage (Strongly Recommended)
    * Some of the data sources can grow to be quite large and require very large shuffle stages which requires
    sufficient local disk. If you choose not to use auto-scaling storage be sure you provisions sufficient local
    disk space.
    * SSD or NVME -- It's strongly recommended to use fast local disks as there can be very large shuffles

### Run via Notebook
Example Notebook ([HTML](assets/GettingStarted/Runner_Job.html) / [DBC](/assets/GettingStarted/Runner_Job.dbc)) -- 
Create a notebook like the one referenced and substitute your 
[**configuration**]({{%relref "UserGuide/Configuration.md"%}}) overrides.

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

import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver}
import com.databricks.labs.overwatch.utils._

private val dataTarget = DataTarget(Some(db), Some(s"dbfs:/user/hive/warehouse/${db}.db"))
private val tokenSecret = TokenSecret("databricks", "overwatch_key")

val params = OverwatchParams(
  auditLogConfig = AuditLogConfig(Some("s3a://auditLogBucketMount/audit-logs")),
  tokenSecret = Some(tokenSecret),
  dataTarget = Some(dataTarget), 
  badRecordsPath = Some(s"/tmp/tomes/overwatch/sparkEventsBadrecords"),
  overwatchScope = Some("audit,accounts,sparkEvents,jobs,clusters,clusterEvents,notebooks".split(",")),
  maxDaysToLoad = 30
)

private val args = JsonUtils.objToJson(params).compactString
val workspace = Initializer(Array(args), debugFlag = false)

Bronze(workspace).run()
Silver(workspace).run()
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
{{% notice warning%}}
TODO - These images must be anonymized
{{% /notice %}}
![job_params](/images/GettingStarted/job_params.png) <br>
![jarSetupExample](/images/GettingStarted/jarSetupExample.png)

### Modules
Choosing which modules are right for your organization can seem a bit overwhelming, but for now it should be 
quite simple as several modules aren't yet released in this version. "Modules" are also sometimes 
referred to as "Scope", know that the meaning is synonymous. To understand which modules are available in your
version and get more details on what's included, please refer to [Modules]({{%relref "UserGuide/Modules.md"%}}) 

### Security Considerations
Overwatch will create a database that, by default, is accessible to everyone in your organization unless you
specify a location for the database in the configuration that is secured. Several of the modules capture fairly
sensitive data such as users, userIDs, etc. and the higher level data models (such as gold) make it simple for 
users to identify costs by user/group along with commonly access data sources for a specific user/group. 

At present, Overwatch doesn't allow you 
to specify security configurations but simplified security setup is on the roadmap. For now, if you desire 
a database that is not accessible by all users, it's recommended to point the database target location to a 
specific bucket/storageAcct that's specifically for Overwatch. From there, the standard Databricks security
options can be employed to allow specific groups of users to access specific tables.
