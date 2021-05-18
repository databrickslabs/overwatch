---
title: "Getting Started"
date: 2020-10-27T16:40:00-04:00
weight: 1
---

## Run Methods
Overwatch is usually run via one of two methods:
1. As a job controlling [**running a notebook**](#executing-overwatch-via-notebook)
2. As a job running the [**main class**](#executing-overwatch-via-main-class) with the configs

Below are instructions for both methods. The primary difference is the way by which the
[configuration]({{%relref "GettingStarted/Configuration.md"%}}) is implemented and the choice of which to use is
entirely user preference. Overwatch is meant to control itself from soup to nuts meaning it creates its own databases
and tables and manages all spark parameters and optimization requirements to operate efficiently. Beyond the config
and the job run setup, Overwatch runs best as a black box -- enable it and forget about it.

## Cluster Requirements
* DBR 7.6
* Add the relevant [dependencies](#cluster-dependencies)
* Azure - Spot Instances not yet tested

### Cluster Dependencies
Add the following dependencies to your cluster
* Overwatch Assembly (fat jar): `com.databricks.labs:overwatch_2.12:<latest>`
  * Note if using a custom Jar, one not in Maven, it may not have the scalaj dependency so be sure to add it as well
  or the API calls will fail `org.scalaj:scalaj-http_2.12:2.4.2`
* (Azure Only) azure-eventhubs-spark - integration with Azure EventHubs
    * Maven Coordinate: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18`

### Notes on Autoscaling
* Auto-scaling compute -- **Not Recommended**
  Note that autoscaling compute will not be extremely efficient due to some of the compute tails
  as a result of log file size skew and storage mediums. Additionally, some modules require thousands of API calls
  (for historical loads) and have to be throttled to protect the workspace. As such, it's recommended to not use
  autoscaling at this time, but rather use a smaller cluster and just let Overwatch run. Until [Issue 16](https://github.com/databrickslabs/overwatch/issues/16)
  can be implemented there will be a few inefficiencies.
* Auto-scaling Storage -- **Strongly Recommended** for historical loads
  * Some of the data sources can grow to be quite large and require very large shuffle stages which requires
    sufficient local disk. If you choose not to use auto-scaling storage be sure you provision sufficient local
    disk space.
    * SSD or NVME (preferred) -- It's strongly recommended to use fast local disks as there can be very large shuffles

## Environment Setup
There are some basic environment configuration steps that need to be enabled to allow Overwatch to do its job.

These activities include things like [enabling Audit Logs](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html), setting up security (as desired), etc. The setup activities
are slightly different depending on your cloud provider, currently Overwatch can operate on
[**aws**]({{%relref "EnvironmentSetup/AWS.md"%}}) and [**azure**]({{%relref "EnvironmentSetup/Azure.md"%}}). Please
reference the page that's relevant for your deployment.

#### Estimated Spot Prices
Overwatch doesn't automatically estimate or calculate spot prices. If spot price estimation is desired, best practice 
is to review the historical spot prices by node in your region and create a lookup table for referencing node type 
by historical spot price detail. This will have to be manually discounted from compute costs later.

### Initializing the Environment
It's best to run the first initialize command manually to validate the configuration and ensure the costs are built
as desired. This should only be completed one time per target database (often many workspaces to one 
target database, the targets are then mounted on several workspaces to which they are appended by the Overwatch 
processes).

In a notebook, run the following commands (customized for your needs of course) to initialize the targets. For 
additional details on config customization options see [**configuration**]({{%relref "GettingStarted/Configuration.md"%}}).

**Example Notebooks:**
The following notebooks will demonstrate a typical best practice for multi-workspace (and stand-alone) configurations. 
Simply populate the necessary variables for your environment.
* AWS ([HTML](/assets/GettingStarted/aws_runner_docs_example.html) / [DBC](/assets/GettingStarted/aws_runner_docs_example.dbc))
* AZURE ([HTML](/assets/GettingStarted/azure_runner_docs_example.html) / [DBC](/assets/GettingStarted/azure_runner_docs_example.dbc))

The code snippet below will initialize the workspace.
```scala
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

private val args = JsonUtils.objToJson(params).compactString
val workspace = if (args.length != 0) {
  Initializer(Array(args), debugFlag = true)
} else { Initializer(Array()) }
```

If this is the first run and you'd like to customize compute costs to mirror your region / contract price, be sure to 
run the following to initialize the pipeline which will in turn create the instanceDetails table, which you can edit 
/ complete according to your needs. This is only necessary for first-time init with custom costs. More information below 
in the [Configuring Custom Costs](#configuring-custom-costs) section.
```scala
Bronze(workspace)
```

This is also a great time to materialize the JSON configs for use in the Databricks job the main class (not notebook) 
is to be used to execute the job.
```scala
val escapedConfigString = JsonUtils.objToJson(params).escapedString
val prettyConfigString = JsonUtils.objToJson(params).compactString
```

The target database\[s\] should now be visible from the data tab in Databricks. Additionally, the consumer database 
should contain the [InstanceDetails]({{%relref "DataEngineer/Definitions.md"%}}/#instancedetails) table. This is the 
time to make customizations to this table, after first init but before first pipeline run.

### Configuring Custom Costs
There are three essential components to the cost function:
* The node type and its associated contract price
* The node type and its associated DBUs per hour
* The DBU contract prices for both interactive and automated.

The two DBU contract costs are captured from the Overwatch run config; however, compute costs and dbu to node
associations are maintained in the [InstanceDetails]({{%relref "DataEngineer/Definitions.md"%}}/#instancedetails) table.
**IMPORTANT** This table is automatically created in the target database upon first initialization of the pipeline. 
**DO NOT** try to manually create the target database outside of Overwatch as that will lead to database validation errors. 

**To customize costs** using this table run the [first initialization](#initializing-the-environment) as detailed above 
including the pipeline initialization (e.g Bronze(workspace) - DO NOT call the .run function yet).
Note that each subsequent workspace referencing the same etlDataPathPrefix on which you execute Overwatch, the 
default costs by node will be appended to instanceDetails table if no data is present for that organization_id 
(i.e. workspace). If you would like to customize compute costs for all workspaces,   
export the instanceDetails dataset to external editor (after first init), add the required metrics mentioned above 
for each workspace, and overwrite the target table with the customized cost information. Note that the instanceDetails 
object in the consumer database is just a view so you must edit/overwrite the table in the ETL database. The view 
will automatically be recreated upon first pipeline run.

[Helpful Tool (AZURE_Only)](https://azureprice.net/) to get pricing by region by node.


{{< rawhtml >}}
<a href="https://drive.google.com/file/d/1tj0GV-vX1Ka9cRcJpJSwkQx6bbpueSwl/view?usp=sharing" target="_blank">AWS Example</a>
{{< /rawhtml >}} |
{{< rawhtml >}}
<a href="https://drive.google.com/file/d/13hYZrOAmzLwIjfgNz0YWx-qE2TdWe0-c/view?usp=sharing" target="_blank">Azure Example</a>
{{< /rawhtml >}}

### Executing Overwatch via Notebook
Continuing from where we left off above in [Initializing The Environment](#initializing-the-environment), we'll 
make sure we have the workspace variable and then we can run the pipeline layers in order. Note that when executing
via a notebook, the pretty / compact arguments String should be used (not the escaped string).
```scala
private val args = JsonUtils.objToJson(params).compactString
val workspace = Initializer(Array(args), debugFlag = false)

Bronze(workspace).run()
Silver(workspace).run()
Gold(workspace).run()
```

### Executing Overwatch via Main Class
The Main class version does the same thing as the notebook version the difference is just how it's kicked off and
configured. The tricky party about the main class is that the parameters must be passed in as a json array of a strings 
with one length and escaped. The "OverwatchParams" object gets serialized and passed into the args of the Main Class. To
simplify the creation of this string simply follow the example above [Initializing The Environment](#initializing-the-environment)
and use the `escapedConfigString`. Now that you have the string, setup the job with that string as the Parameters. 
The Main Class for histoical batch is `com.databricks.labs.overwatch.BatchRunner`<br>

![job_params](/images/GettingStarted/job_params.png) <br>
![jarSetupExample](/images/GettingStarted/jarSetupExample.png)

#### The new Jobs UI
There's a new jobs UI being rolled out around the same time as Overwatch and to use it there is a slight difference, the 
string needs to be wrapped inside an array, thus the escapedConfigString should be surrounded by `["<escapedConfigString>"]`
where <escapedConfigString> is replaced with the actual parameters string. The images below should help clarify.

![newUIJarSetup](/images/GettingStarted/jarSetupNewUI.png) <br>
![newUIJarSetup](/images/GettingStarted/OverwatchViaMaven.png)

### Modules
Choosing which modules are right for your organization can seem a bit overwhelming, but for now it should be
quite simple as several modules aren't yet released in this version. "Modules" are also
referred to as "Scope", the meaning is synonymous. To understand which modules are available in your
version and get more details on what's included, please refer to [Modules]({{%relref "GettingStarted/Modules.md"%}})

### Security Considerations
Overwatch, by default, will create a single database that, is accessible to everyone in your organization unless you
specify a location for the database in the configuration that is secured at the storage level. Several of the modules
capture fairly sensitive data such as users, userIDs, etc. It is suggested that the configuration specify two
databases in the configuration:
* **ETL database** -- hold all raw and intermediate transform entities. This database can be secured
  allowing only the necessary data engineers direct access.
* **Consumer database** -- Holds views only and is easy to secure using Databricks' table ACLs (assuming no direct
  scala access). The consumer database holds only views that point to tables so additional security can easily be
  attributed at this layer.

For more information on how to configure the separation of ETL and consumption databases, please reference the
[**configuration**]({{%relref "GettingStarted/Configuration.md"%}}) page.

Additional steps can be taken to secure the storage location of the ETL entities as necessary. The method for
securing access to these tables would be the same as with any set of tables in your organization.
