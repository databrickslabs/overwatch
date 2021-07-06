---
title: "ChangeLog"
date: 2021-05-05T17:00:13-04:00
weight: 4
---
## 0.4.2
### Upgrading and Required Changes
* **[SCHEMA UPGRADE]({{%relref "DataEngineer/Upgrade.md"%}}) REQUIRED** - If you are upgrading from 0.4.12+ please use *Upgrade.Upgrade0420* If upgrading from a schema 
  prior to 0.4.12, please first upgrade to 0.4.12 and then to 0.4.2. The upgrade process will be improving or going away 
  soon, please bear with us while we improve this process.
```scala
import com.databricks.labs.overwatch.utils.Upgrade
val upgradeReport = Upgrade.upgradeTo042(<YOUR WORKSPACE OBJECT>)
```
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
    need to be rolled back as per the documents in [ETL Process]({{%relref "DataEngineer/ETL_Process.md"%}})
* DBU Contract Costs for DatabricksSQL and JobsLight added
  * Costing for additional SKUs were added to the configuration such that they can be tracked. Note that as of 0.4.2 
  release, no calculation changes in costing as it relates to sku have yet been incorporated. These recalculations for
  jobs light and DatabricksSQL are in progress.
* Enabled [Main class execution]({{%relref "GettingStarted"%}}/#executing-overwatch-via-main-class) to execute only a single layer of the pipeline such as bronze / silver / gold. Primarily 
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


## 0.4.13
* Hotfix for [Issue 138](https://github.com/databrickslabs/overwatch/issues/138)

An upgrade to this minor release is only necessary if you're experience api limits and/or seeing 429 issues. 
Otherwise, this release can be skipped as the fix will be in 0.4.2+. New users should use this version until 
the next release is published.

## 0.4.12
**[SCHEMA UPGRADE]({{%relref "DataEngineer/Upgrade.md"%}}) REQUIRED**
* Hotfix for [Issue 126](https://github.com/databrickslabs/overwatch/issues/126).
* Hotfix for [Issue 129](https://github.com/databrickslabs/overwatch/issues/129).
* Schema upgrade capability enabled.
  * To upgrade your schema to 0.412, all you need to do is follow the 
  example in the notebook below.
    * ([HTML](/assets/ChangeLog/Upgrade_Example.html) / [DBC](/assets/ChangeLog/Upgrade_Example.dbc))
* Corrected case sensitivity issue. When failure would occur in spark_events_bronze, there were column references 
  to state tables with mismatched column name case sensitivity causing errors
* Corrected issue with missing quotes around update statement to track failed files in 
  spark_events_processedFiles

## 0.4.11
* Hotfix for [Issue 119](https://github.com/databrickslabs/overwatch/issues/119). Issue was only present in edge cases.
  Edge cases include workspaces missing data and attempting to run modules for which data didn't exist.
* Added additional guard rails to pipeline state checks to help clarify workspace / pipeline state issues for
  future users.

## 0.4.1
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

## 0.4.0
Initial Public Release