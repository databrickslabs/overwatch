---
title: "ChangeLog"
date: 2021-05-05T17:00:13-04:00
weight: 4
---
## 0.7.0.0.4 (PATCH)
This is a patch for 0.7.0.0. There were a few issues identified with 0.7.0 by our early movers. This patch is
the same as 0700 with the bug fixes closed in [PR 633](https://github.com/databrickslabs/overwatch/pull/633).

* This update includes all bug fixes published in [07003](#07003-patch).
* No upgrade needed - the Jars may simply be swapped out -- do this on all workspaces.
* If you were affected by one of the issues fixed in the PR above and you would like to repair the historical data,
  please do so using this script. Instructions are in the notebook.
  [HTML](/assets/ChangeLog/07004_data_repair.html) | [DBC](/assets/ChangeLog/07004_data_repair.dbc)
  
### Bug Fixes
* Bug fix related to dbu utilization and cost calculation [Issue 632](https://github.com/databrickslabs/overwatch/issues/632).
* Bug fix related to sqlQueryHistory [Issue 625](https://github.com/databrickslabs/overwatch/issues/625).

## 0.7.0.0.3 (PATCH)
This is a patch for 0.7.0.0. There were a few issues identified with 0.7.0 by our early movers. This patch is
the same as 0700 with the bug fixes closed in [PR 602](https://github.com/databrickslabs/overwatch/pull/602).
* This update includes all bug fixes published in [07002](#07002-patch)
* No upgrade needed - the Jars may simply be swapped out -- do this on all workspaces
* If you were affected by one of the issues fixed and you would like to repair the historical data,
  please do so using this script. Instructions are in the notebook.
  [HTML](/assets/ChangeLog/0700x_data_repair.html) | [DBC](/assets/ChangeLog/0700x_data_repair.dbc)


## 0.7.0.0.2 (PATCH)
This is a patch for 0.7.0.0. There were a few issues identified with 0.7.0 by our early movers. This patch is
the same as 0700 with the bug fixes closed in [PR 580](https://github.com/databrickslabs/overwatch/pull/580).
* This update includes all bug fixes published in [07001](#07001-patch)
* No upgrade needed - the Jars may simply be swapped out -- do this on all workspaces
* If you were affected by one of the issues fixed in the PR above and you would like to repair the historical data, 
  please do so using this script. Instructions are in the notebook. 
  [HTML](/assets/ChangeLog/0700x_data_repair.html) | [DBC](/assets/ChangeLog/0700x_data_repair.dbc)
  
## 0.7.0.0.1 (PATCH)
This is a patch for 0.7.0.0. There were a few issues identified with 0.7.0 by our early movers. This patch is 
the same as 0700 with the bug fixes closed in [PR 559](https://github.com/databrickslabs/overwatch/pull/559).

## 0.7.0.0 (Major Release)
**Please use [PATCH 0.7.0.0.1](#07001-patch)** from maven. Everything in this release stands

**0700 Upgrade Script** [HTML](/assets/ChangeLog/Upgrade_070_Template.html) | [DBC](/assets/ChangeLog/Upgrade_070_Template.dbc)
### Major Changes
* **Support for Multi-Task Jobs (MTJs)**
  * Prior to this release Overwatch was unable to properly analyze and display jobs with multiple tasks and, at times
  even struggled to properly display new-format single-task jobs
  * As this is a major change, **there are significant changes in the jobs tables in silver and gold**
  * **Production Dashboards** should be validated/updated in non-prod 0.7.0 deployment as there will likely be some 
  changes to column naming, typing, and structures. For details on the new schema, see the 
  [Data Dictionary]({{%relref "dataengineer/definitions/_index.md"%}})
* **Support for Job Clusters**
* **Jobs 2.1 API Support**
  * Databricks Jobs team has migrated to API 2.1 and now so has Overwatch. This resolves some data gaps as well 
  as API capacity issues
* **Support for DBSQL Query History Including Metrics**
  * DBSQL Query History is our first step into first-party support for DBSQL. Warehouses are expected to follow shortly 
  but was unable to make the cut for this release
  * Be sure to enable the new scope "dbsql" or use the new 
  [0.7.0 Runner Notebook]({{%relref "GettingStarted/"%}}#jump-start-notebooks) to ensure you're loading 
  the DBSQL tables if your workspace[s] are using DBSQL.
  * Limitation - Databricks does not publish Warehouse events yet and as such, explicit cost anlaysis is not yet 
  possible for DBSQL. As soon as this is made available the Overwatch Dev team will begin work to integrate it.
* **Photon Costs Attributed in CLSF and JRCP**
  * Photon cost calculations have been integrated into Overwatch.
  * By default the Photon costs will begin being applied after the upgrade date. If you want to retroactively apply 
  this change you can by rolling back and rebuilding the following tables as this is not automatically handled in 
  the upgrade (some customers don't want to see that change in their historical reports)
    * cluster_spec_silver
    * clusterstatefact_gold 
    * jobruncostpotentialfact_gold
* **New API Manager**
  * The API management libraries were completely rebuilt from the ground up to maximize throughput, capabilities, 
  and safety for your environment.
  * The new API manager offers users much deeper control on how Overwatch uses the APIs. See the 
  [APIEnv Configs]({{%relref "GettingStarted/Configuration.md"%}}/#apienv) on the Configurations details page.
* **Support for Authenticated PROXY**
  * Most customers are able to configure proxies with cluster configs and init scripts but Overwatch now offers 
  deeper, first-party suppot for authenticated Proxy configs. See more details in the
  [APIEnv Configs]({{%relref "GettingStarted/Configuration.md"%}}/#apienv) section.
* **SparkJob User-Email Coverage & Accuracy**
  * User_Email in sparkJob table now has complete coverage **but may require some action**
    * The user_email had incompletions and inaccuracies. Databricks was not publishing the user data for users using 
    Python and R languages. This has now been resolved automatically for clusters
    (operational clusters not the Overwatch cluster) running DBR 11.3+. To enable this feature on <= DBR 11.2 
    clusters a config must be added to the cluster. See the 
    [FAQ 9]({{%relref "FAQ/"%}}/#q9-how-to-enable-user-email-tracking-in-sparkjob-table-for-dbr-versions--113) 
    for details on how to ensure you're getting 
    complete coverage for this field.
* **Multi-Workspace Deployment** (DELAYED)
  * Allows customers to deploy, manage, and run Overwatch from a single Workspace while still collecting data from 
  several.
  * Provides unified global configuration via Excel / Delta tables
  * While many customers are eagerly awaiting this feature and the dev team thought it could be released as part of 
  this major release; unfortunately, due to some complications this wasn't possible but should be out in the coming 
  weeks and isn't expected to need any sort of "upgrade", it should be just a JAR swap.
    * More details and full documentation coming soon.

### Bug Fixes
* Biggest fix is with regard to data quality surrounding multi-task jobs and job-clusters. This should all be 
resolved at this point.

Full list of bug fixes for this release can be found [here](https://github.com/databrickslabs/overwatch/milestone/12?closed=1)

## 0.6.1.1 (Maintenance Release)
To upgrade from 0.6.1.0, simply swap the JAR from 0.6.1.0 to this version, no upgrade script necessary

* Enhancements
  * Extreme performance improvement (~75% for large workspaces) for several of the largest silver/gold modules.
  * Increased reliability on AQE for further perf improvements
  * Perf improvements for CLSF windows
  * Bronze workspace snapshot helper function created to backup bronze
  * Duplicates hunter / dropper functions enabled
  
* Bug Fixes
  * Full list of bug fixes for this release can be found [here](https://github.com/databrickslabs/overwatch/issues?q=is%3Aissue+milestone%3A0.6.1.1+)
  

## 0.6.1.0 (Upgrade Release)
**Upgrade Process Required** for existing customers on 0.6.0.x
The upgrade is small and quick but is very important. Some new Databricks features have resulted in some unsafe 
columns (i.e. complex unbound structs) in bronze; thus, these need to be converted to map types to reduce 
schema cardinality. There are three tables affected and the details can be found in the upgrade script linked below.

You are strongly urged to **upgrade DBR version to 10.4LTS** (from 9.1LTS). If you do not upgrade DBR to 10.4LTS, the upgrade can be 
quite compute intensive due to the need to fully rebuild one of the largest tables, `spark_events_bronze`. A new 
feature called, [column mapping](https://docs.databricks.com/delta/delta-column-mapping.html) 
is available in 10.4LTS that doesn't require a full table rebuild. The caveat is 
that all future reads/writes to this table require 10.4LTS+. The ONLY customers not upgrading to 10.4LTS should be those 
with a legitimate business blocker for upgrading to 10.4LTS.

{{% notice note %}}
When upgrading to 0.6.1 using 10.4LTS all future reads/writes to bronze table spark_events_bronze will require a 
cluster with DBR 10.4LTS+
{{% /notice%}}

* **0610 Upgrade Script - 10.4LTS** [HTML](/assets/ChangeLog/Upgrade_0610.html) | [DBC](/assets/ChangeLog/Upgrade_0610.dbc)
* **0610 Upgrade Script - 9.1LTS** [HTML](/assets/ChangeLog/Upgrade_0610_91LTS.html) | [DBC](/assets/ChangeLog/Upgrade_0610_91LTS.dbc)

For questions / issues with the upgrade, please file a [git ticket](https://github.com/databrickslabs/overwatch/issues/new)

* Noteworthy Features
  * Add Overwatch databases and contents to a workspace not running Overwatch (i.e. remote only)
  * Ability to specify custom temporary working directories
    * Used to default to a dir in /tmp but due to some root policies allowing it to be overridden for more details 
    see [Configs]({{%relref "GettingStarted/Configuration.md"%}}/#overwatchparams)
  * Pipeline Management simplifications
* Major Fixes
  * Data Quality Enhancements
  * Pipeline edge case stability improvements
  
The full list of fixes can be found on the [0610 Milestone](https://github.com/databrickslabs/overwatch/milestone/11?closed=1)

## 0.6.0.4 (Maintenance Release)
* Bug fixes
  * [Issue 305](https://github.com/databrickslabs/overwatch/issues/305) - Silver_jobStatus timeout_seconds schema type
  * [Issue 308](https://github.com/databrickslabs/overwatch/issues/308) - Optimize job could get the wrong workspace 
  credentials in some cases
  * [Issue 310](https://github.com/databrickslabs/overwatch/issues/310) - SparkJob table, many nulled fields
  * [Issue 313](https://github.com/databrickslabs/overwatch/issues/313) - Silver_ClusterStateDetail - no column 
  autoscale
    

## 0.6.0.3 (Deprecated)
If an issue is found in this version, please try the latest 0.6.x version, if the issue persists, please open a 
[git ticket](https://github.com/databrickslabs/overwatch/issues/new)
* Fixes regression found in [0602](#0602----deprecated----dont-use)
  * [Issue 297](https://github.com/databrickslabs/overwatch/issues/297)
  * **If you are affected**, please **use the script** linked below to cleanup the historical data. If you'd like assistance, 
  please file a [git ticket](https://github.com/databrickslabs/overwatch/issues/new) and someone will review the process with you.
    * CLEANUP SCRIPT ([DBC](/assets/ChangeLog/0602_Spark_Repair.dbc) | [HTML](/assets/ChangeLog/0602_Spark_Repair.html))
* Minimum Events for Azure audit log stream ingest -- improvement for very busy workspaces
  * [Issue 299](https://github.com/databrickslabs/overwatch/issues/299)
* Resolves issue where "init_scripts" field is missing
  * Workspaces that have never had a cluster with an init script could hit this issue
* Performance Improvement for Azure Audit log ingestion
* Improved Schema enforcement
* Fix for ArrayType minimum schema requirements

## 0.6.0.2 -- DEPRECATED -- DONT USE
Contains data quality regression - See [Issue 297](https://github.com/databrickslabs/overwatch/issues/297)
Several bug fixes - mostly edge cases

A full list of resolutions can be found [here](https://github.com/databrickslabs/overwatch/milestone/7?closed=1)

## 0.6.0.1 (MAJOR UPGRADE/RELEASE)
**Existing Customers on 0.5.x --> 0.6.x [UPGRADE PROCESS REQUIRED]({{%relref "DataEngineer/Upgrade.md"%}})**
* Upgrade process detailed in linked upgrade script [**HTML**](/assets/ChangeLog/060_upgrade_process.html) | 
  [**DBC**](/assets/ChangeLog/060_upgrade_process.dbc)
  
Notice -- 0.6.0 had a concurrent merge conflict for some multi-workspace deployments described in 
[issue 284](https://github.com/databrickslabs/overwatch/issues/284). When upgrading to 0.6 please use 0.6.0.1 as the 
base version, 0.6.0 shouldn't be used.

**Noteworthy Features**
* Custom workspace names
* Upserts - Previous versions only appended data which meant that the run / clusterState / etc had to end before 
the Overwatch job would pick it up. Now Overwatch utilizes delta upserts/merges so all data comes into the pipeline 
anchored on start_time not end_time.
* Numerous and Significant Schema enhancements in Gold/Consumer layer
  * Increases resiliency and simplifies analysis
* New Entities in Consumer Layer
  * InstancePools
  * Streams (preview) -- Releasing as preview to get more feedback and improve structure for use cases.
  * DBSQL - slipped -- DBSQL entities did not make it to gold in 0.6.0 but are staged to be released in next minor release
* Separated node (compute) contract costs from dbu contract costs
  * DBU costs by sku slow-changing-dimension added (dbuCostDetails)
  * Old *instanceDetails* table greatly simplified. Name is still the same, only the structure changed
* Enabled externalized optimize & z-order pipelines
  * This was and is baked into the Overwatch process but as of 0.6.0 users can decide to externalize this. This  
  is powerful as it allows the Overwatch pipeline to be more efficient and the Optimize/Zorder job to be completed 
  from a single workspace with the appropriate cluster size/configuration for the workload.
* Gold Table Initializations
  * In previous version there were often significant gaps in early runs of Overwatch as the objects that create the 
  slow-changing-dims weren't created/edited thus were absent from the audit logs. The APIs are now used to get an 
  initial full picture of the workspace and that is merged with the audit logs to jump-start all records for early 
  runs of Overwatch
* Error Message Improvements
  * The error messaging pipelines have been signficantly improved to provide more useful and specific errors
* All tables fully externalized to paths
  * Enables finer-grained security for multi-workspace environments
  
0.6.0.1 also brings numerous data quality enhancements, performance optimization, and resolves several outstanding bugs

Please note that pipelines may run a bit longer in some cases as we are now executing merges instead of appends. 
Merges are a bit more compute intensive and future optimizations are already in the works to minimize the increase. 
The increases were marginal but noteworthy.

A more complete list of features and improvements can be found on the closed features/bugs list of 
[Milestone 0.6.0](https://github.com/databrickslabs/overwatch/milestone/6?closed=1)

## Version 0.5.x NOTE
If you’re on 0.5.x and not ready to upgrade to 0.6.0 yet, and you’re not on the latest version of 0.5.x please do 
upgrade to the latest 0.5.x as 0.5.x will no longer be receiving feature updates and is considered stable at 
the latest version. No upgrade necessary, just swap the JAR to the latest version of 0.5.x and everything will 
work even better.

### Regarding Deprecated Versions
Please note that we're a pretty small team trying to keep up with a wide customer base. If the version you're on 
shows deprecated, please note thta it doesn't mean that we won't try to offer assistance, just that we may first ask 
that you swap the jar out to the latest version of 0.5.x before we do a deep dive as many of the issues you may face 
are already resolved in the later version. Please do upgrade to the latest release as soon as possible.

## 0.5.0.6.1
Apologies for the insanity of the version number -- will be better in future
* Patch for [Issue_278](https://github.com/databrickslabs/overwatch/issues/278)
  * Databricks published some corrupted log files. These log files had duplicate column names with upper and lower 
  case references. This was promptly resolved on the Databricks side but the logs were not retroactively created; 
  thus, if you hit the error below using 0506 or earlier, you will need to switch to this JAR for your pipeline to 
  continue.
    
```AnalysisException: Found duplicate column(s) in the data schema: <column_name>```

## 0.5.0.6
* Patch for [Issue_235](https://github.com/databrickslabs/overwatch/issues/235)
  * Edge cases resulted in nulls for several values in clusterstatefact.

## 0.5.0.5 - deprecated
* Feature [Feature_223](https://github.com/databrickslabs/overwatch/issues/223)
  * Adds package version to parsed config in pipeline_report
* Patch fix for [Issue_210](https://github.com/databrickslabs/overwatch/issues/210)
* Patch fix for [Issue_214](https://github.com/databrickslabs/overwatch/issues/214)
  * Important fix for Spark Users -- this bug resulted in several spark logs not getting ingested
* Patch fix for [Issue_218](https://github.com/databrickslabs/overwatch/issues/218)
  * Potential duplicates for api call "RunNow"
* Patch fix for [Issue_221](https://github.com/databrickslabs/overwatch/issues/221)
  * Multi-workspace enhancements for validations
  * Altered DBU Costs fix
* Patch fix for [Issue_213](https://github.com/databrickslabs/overwatch/issues/213)
  * Reduced plan size for jobRunCostPotentialFact table build
* Patch fix for [Issue_192](https://github.com/databrickslabs/overwatch/issues/192)
  * Some spark overrides in the Overwatch pipeline were not getting applied 
* Patch fix for [Issue_206](https://github.com/databrickslabs/overwatch/issues/206)
  * Incorrect default prices for several node types

## 0.5.0.4 - deprecated
* Patch fix for [Issue_196](https://github.com/databrickslabs/overwatch/issues/196)
* AZURE - Enhancement - Enable full EH configuration to be passed through the job arguments to the main class using 
secrets / scopes -- [Issue_197](https://github.com/databrickslabs/overwatch/issues/197)

## 0.5.0.3 - deprecated
{{% notice warning %}}
**BUG FOR NEW DEPLOYMENTS**
When using 0.5.0.3 for a brand new deployment, a duplicate key was left in a static
table causing a couple of gold layer costing tables to error out. If this is your first run, the following is
a fix until 0.5.0.4 comes out with the fix.
{{% /notice %}}

* [Initialize the Environment]({{%relref "GettingStarted/"%}}#initializing-the-environment) -- note this is the standard
  getting started process. The only difference is to stop after Bronze(workspace) and not continue.
  * Execute through "Bronze(workspace)" **DO NOT RUN THE PIPELINE** (i.e. Don't use Bronze(workspace).run()), just 
  initialize it.
* Execute the following command in SQL to delete the single offending row. Remember to replace <overwatch_etl> with your
  configured ETL database
```sql
delete from <overwatch_etl>.instancedetails where API_Name = 'Standard_E4as_v4' and On_Demand_Cost_Hourly = 0.1482
```
* Resume standard workflow

**Bug Fixes**
* Incremental input sources returning 0 rows when users run Overwatch multiple times in the same day
* Incremental clusterStateFactGold -- addresses orphaned events for long-running states across Overwatch Runs.
* null driver node types for pooled drivers
* Improved instance pool lookup logic
* Mixed instance pools enabled
* basic refactoring for sparkOverrides -- full refactor in 0.5.1
* Improved logging
* Costing - node details lookups for edge cases -- there was a bug in the join condition for users running Overwatch multiple times per day in the same workspace

## 0.5.0.2 - deprecated
**If upgrading from Overwatch version prior to [0.5.0](#050) please see schema upgrade requirements**
* Hotfix release to resolve [Issue 179](https://github.com/databrickslabs/overwatch/issues/179).
  * clusterstatefact_gold incremental column was start_timestamp instead of end_timestamp. Meant to roll this into
  0.5.0.1 but it got missed, sorry for the double release.

## 0.5.0.1 - deprecated
**If upgrading from Overwatch version prior to [0.5.0](#050) please see schema upgrade requirements**
* Hotfix release to resolve [Issue 170](https://github.com/databrickslabs/overwatch/issues/170).
* Hotfix to resolve laggard DF lookups required to join across pipeline runs.
  * Caused some events to not be joined with their lagging start events in the data model.
* Hotfix to enable non-json formatted audit logs (AWS) primarily for PVC edge cases

## 0.5.0 - deprecated
### Upgrading and Required Changes
* **[SCHEMA UPGRADE]({{%relref "DataEngineer/Upgrade.md"%}}) REQUIRED** - If you are upgrading from 0.4.12+ please 
  use *Upgrade.UpgradeTo0420* If upgrading from a schema 
  prior to 0.4.12, please first upgrade to 0.4.12 and then to 0.4.2. The upgrade process will be improving or going away 
  soon, please bear with us while we improve this process.
```scala
import com.databricks.labs.overwatch.utils.Upgrade
val upgradeReport = Upgrade.upgradeTo042(<YOUR WORKSPACE OBJECT>)
```

{{% notice note %}}
The schema versions have been tied to the release versions until 0.5.0, this divergence was intentional. A schema 
version should not == the binary release version as changes to the schema should be much more infrequent than 
binary releases. Rest assured binary v0.5.0 requires schema version 0.4.2. Please upgrade to 0.4.2 as noted above 
using the 0.5.0 binary but before the pipeline is executed.
{{% /notice %}}

* **Launch Config Change** - Note the difference in the *Initializer* for 0.4.2. The following change is ONLY necessary when/if 
  instantiating Overwatch from a notebook, the main class does not require this change. Sorry for the breaking change 
  we will try to minimize these everywhere possible
```scala
// 0.4.1 (OLD)
val workspace = Initializer(Array(args), debugFlag = true)
// 0.4.2+ (NEW)
val workspace = Initializer(args, debugFlag = true)
```

### Major Features & Enhancements
Below are the major feature and enhancements in 0.4.2
* Improved [Getting Started Example Notebooks]({{%relref "GettingStarted"%}}/#initializing-the-environment)
* Kitana
  * Testing framework, currently meant for advanced usage, documentation will roll out asap.
* [Intelligent scaling]({{%relref "GettingStarted/configuration.md"%}}/#intelligentscaling)
* [Audit Log Formats (AWS)](https://github.com/databrickslabs/overwatch/issues/151)
  * For AWS the input format of the audit logs is now configurable to json (default), delta, parquet
* [Contract costs through time](https://github.com/databrickslabs/overwatch/issues/49)
  * Changes to the instanceDetails table now allows for the tracking of all costs through time including compute and 
  DBU. To recalcuate associated costs in the gold layer, the jobRunCostPotentialFact and clusterStateFact table will 
    need to be rolled back as per the documents in [Pipeline_Management]({{%relref "DataEngineer/Pipeline_Management.md"%}})
* DBU Contract Costs for DatabricksSQL and JobsLight added
  * Costing for additional SKUs were added to the configuration such that they can be tracked. Note that as of 0.4.2 
  release, no calculation changes in costing as it relates to sku have yet been incorporated. These recalculations for
  jobs light and DatabricksSQL are in progress.
* Enabled [Main class execution]({{%relref "GettingStarted"%}}/#via-main-class) to execute only a single layer of the pipeline such as bronze / silver / gold. Primarily 
  enabled for future enablement with jobs pipelines and for development / testing purposes.
  * User can now pass 2 arguments to the databricks job where the first is 'bronze', 'silver', or 'gold' and the second
  is the escaped configuration string and only that pipeline layer will execute.
* Event Hub Checkpoint - Auto-Recovery (Azure)
  * Previously, if an EH state was to get lost or corrupted, it was very challenging to resume. Overwatch will now
  automatically identify the latest state loaded and resume.
* [Support non-dbfs file types](https://github.com/databrickslabs/overwatch/issues/133) 
  such as abfss://, s3a://, s3n:// etc. for audit log source paths and output paths.
  All file types supported on Databricks should now be supported in Overwatch.

### Minor Bug Fixes / Enhancements
* Unsafe SSL allowed [Issue 152](https://github.com/databrickslabs/overwatch/issues/152). 
  All API connections will first be attempted using standard SSL but will fallback on unsafe 
if there is a SSLHandshakeException. Targeted at specific scenarios.
* ERD Updates for accuracy
* Issue resolved where not all cluster logs were being loaded thus several spark log events were missing
* Various schema errors in edge use cases
* Docs updates and ehancements

### Additional Fixes
* non-dbfs uris for cluster logs (i.e. s3a://, abfss://)
* intelligent scaling calculation
* improved errors for access issues with api calls
* unsafeSSL fallback optimization and bug fix
* elementType implicit casting to target schemas within arrays
* aws - non-json formatted audit logs schema unification
* api schema scrubber improvements for addtional edge cases


## 0.4.2 - deprecated
* Deprecated release - please use [0.5.0](#050)
* The 0.4.2 can be viewed as an RC for 0.5.0. The 0.5.0 release is the 0.4.2 release with several bug fixes

## 0.4.13 - deprecated
* Hotfix for [Issue 138](https://github.com/databrickslabs/overwatch/issues/138)

An upgrade to this minor release is only necessary if you're experience api limits and/or seeing 429 issues. 
Otherwise, this release can be skipped as the fix will be in 0.4.2+. New users should use this version until 
the next release is published.

## 0.4.12 - deprecated
**[SCHEMA UPGRADE]({{%relref "DataEngineer/Upgrade.md"%}}) REQUIRED**
* Hotfix for [Issue 126](https://github.com/databrickslabs/overwatch/issues/126).
* Hotfix for [Issue 129](https://github.com/databrickslabs/overwatch/issues/129).
* Schema upgrade capability enabled.
  * Please follow the [upgrade documentation]({{%relref "DataEngineer/Upgrade.md"%}}) to complete your upgrade
  * Upgrade function name == `Upgrade.upgradeTo0412`
* Corrected case sensitivity issue. When failure would occur in spark_events_bronze, there were column references 
  to state tables with mismatched column name case sensitivity causing errors
* Corrected issue with missing quotes around update statement to track failed files in 
  spark_events_processedFiles

## 0.4.11 - deprecated
* Hotfix for [Issue 119](https://github.com/databrickslabs/overwatch/issues/119). Issue was only present in edge cases.
  Edge cases include workspaces missing data and attempting to run modules for which data didn't exist.
* Added additional guard rails to pipeline state checks to help clarify workspace / pipeline state issues for
  future users.

## 0.4.1 - deprecated
* [Converted all delta targets from managed tables to external tables](https://github.com/databrickslabs/overwatch/issues/50)
    * Better security and best practice
    * Enabled etl data path prefix to simplify and protect the raw data across workspaces
* pipeline_report - enabled partitioning by "organization_id"
    * Necessary to guard against write concurrency issues across multiple workspaces
* orgianization_id - Single Tenant AWS workspaces with DNS would return "0" for organization_id, updated to change
  organization_id to the workspace prefix.
    * EXAMPLE: https://myorg_dev.cloud.databricks.com --> myorg_dev
* instanceDetails -
    * Converted from append mode to overwrite mode. Allows for multiple workspaces to create the compute pricing lookup
      if it didn't already exist for the workspace
    * Moved default table location from consumerDB to ETLDB and created mapped view to table in consumerDB
    * Simplified the instanceDetails cost customization process (docs updated)
    * Added proper filter to ensure ONLY current workspace compute costs are looked up during joins
    * Converted Azure example Memory_GB to double to match data type from AWS
* Bug fix for JSON to Map that caused certain escaped json strings to break when being pulled from the new Jobs UI
* Bug fix for [Issue 111](https://github.com/databrickslabs/overwatch/issues/111)
* Bug fix for multi-workspace Azure, filter from audit_raw_land table was incomplete
* Bug fix to allow for consumerDB to be omitted from DataTarget in config

## 0.4.0 - deprecated
Initial Public Release