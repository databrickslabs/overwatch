---
title: "ETL Process"
date: 2021-01-11T12:21:46-05:00
weight: 2
---

## Data Ingestion and Resume Process
The specificities can vary slightly between cloud provider but the general methodology is the exact same. 
Specific differences will be discussed in [Cloud-Specific Variations](#cloud-specific-variations) section. 

Each module is responsible for building certain entities at each layer, bronze, silver, gold, and presentation.
The mapping between module and gold entity can be found in the [Modules]({{%relref "GettingStarted/Modules.md"%}}) section.
Each module is also tracked individually in the primary tracking tabe, [**Pipeline_Report**](#the-pipeline-report).

### The Overwatch Execution Process
Each Overwatch Run takes a snapshot at a point in time and will be the uppermost timestamp for which any data will
be captured. Each Overwatch Run also derives an associated GUID by which it can be globally identified throughout
the run. Each record created during the run will have an associated *Overwatch_RunID* and *Pipeline_SnapTS* which can 
be used to determine exactly when a record was loaded.

The "from_time" will be derived dynamically for each module as per the previous run snapshot time
for the given module plus one millisecond. If the module has never been executed, the primordialDateString will be
used which is defined as current_date - 60 days, by default. This can be overridden in the 
[config]({{%relref "GettingStarted/Configuration.md"%}}). Some modules can be very slow / time-consuming on the 
first run due to the data volume and/or trickle loads from APIs. Best practice is to set your primordialDateString 
(if desired > 60d) AND set "*maxDaysToLoad*" to a low number like 5. It's important to balance sufficient data to 
fully build out the implicit schemas and small enough data to get through an initial valid test.

More best practices can be found in the [Best Practices]({{%relref "GettingStarted/AdvancedTopics.md"%}}#best-practices) 
section; these are especially important to reference when getting started.

## The Pipeline Report
The Pipeline_Report table controls the start/stop points for each module and tracks the status of each run. 
Manipulating this table will change the way Overwatch executes so be sure to read the rest of this page before 
altering this table.

The Pipeline Report is very useful for identifying issues in the pipeline. Each module, each run is detailed here. 
The structure of the table is outlined below.

### Pipeline_Report Structure
[**SAMPLE**](/assets/TableSamples/pipeline_report.tab)

**KEY** -- organization_id + moduleID + Overwatch_RunID

For the developers -- This output is created as a DataSet via the Case Class
*com.databricks.labs.overwatch.utils.ModuleStatusReport*

Column | Type | Description
:---------------------------|:----------------|:--------------------------------------------------
organization_id             |string           |Canonical workspace id
moduleId                    |int              |Module ID -- Unique identifier for the module
moduleName                  |string           |Name of the module
runStartTS                  |long             |Snapshot time of when the Overwatch run begins
runEndTS                    |long             |Snapshot time of when the Overwatch run ends
fromTS                      |long             |The snapshot start time from which data will be loaded for the module
untilTS                     |long             |The snapshot final time from which data will be loaded for the module. This is also the epoch millis of Pipeline_SnapTS
dataFrequency               |string           |(milliSecond OR Daily) Most modules have a time resolution at the millisecond level; however there are certain modules that can only be run daily. This column is used by Overwatch internals to calculate start/stops for new data 
status                      |string           |Terminal Status for the module run (SUCCEEDED, FAILED, ROLLED_BACK, EMPTY, etc). When status is failed, the error message and stack trace that can be obtained is also placed in this field to assist in finding issues 
recordsAppended             |long             |Count of records appended. Not always enabled for performance reasons, see [advanced topics]({{%relref "GettingStarted/AdvancedTopics.md"%}}) for additional information. 
lastOptimizedTS             |long             |Epoch millis of the last delta optimize run. Used by Overwatch internals to maintain healthy and optimized tables.
vacuumRetentionHours        |int              |Retention hours for delta -- How long to retain deleted data
inputConfig                 |struct           |Captured input of OverwatchParams config at time of module run
parsedConfig                |struct           |The config that was parsed into OverwatchParams through the json deserializer
Pipeline_SnapTS             |timestamp        |Timestamp type of the pipeline snapshot time (server time)
Overwatch_RunID             |string           |GUID for the overwatch run

## Overwatch Time
Overwatch will default timezone to that of the server / cluster on which it runs. This is a critical point to 
be aware of when aggregating Overwatch results from several workspaces across different regions.

## Module Dependencies
Several modules depend on other modules as source input; as such, as you can imagine, we don't want modules to 
progress if one of their dependencies is erroring out and not populating any data. To handle this, Modules have
dependencies such that upstream failures will cause downstream modules to be ineligible to continue. There are 
several types of errors that result in a module failure, and those errors are written out in the pipeline_report 
when they occur. Sometimes, there will truly be no new data in an upstream Module, this is ok and will not stop the 
progression of the "until" time as no data is not considered an error. Below are the ETL Modules and their dependencies

Module ID | Module Name | Target Table Name | Module ID Dependencies
:--------------------------|:---|:---|:-----
1001|Bronze_Jobs_Snapshot|jobs_snapshot_bronze|
1002|Bronze_Clusters_Snapshot|clusters_snapshot_bronze|
1003|Bronze_Pools|pools_snapshot_bronze|
1004|Bronze_AuditLogs|audit_log_bronze|
1005|Bronze_ClusterEventLogs|cluster_events_bronze|1004
1006|Bronze_SparkEventLogs|spark_events_bronze|1004
2003|Silver_SPARK_Executors|spark_executors_silver|1006
2005|Silver_SPARK_Executions|spark_executions_silver|1006
2006|Silver_SPARK_Jobs|spark_jobs_silver|1006
2007|Silver_SPARK_Stages|spark_stages_silver|1006
2008|Silver_SPARK_Tasks|spark_tasks_silver|1006
2010|Silver_JobsStatus|job_status_silver|1004
2011|Silver_JobsRuns|jobrun_silver|1004,2010,2014
2014|Silver_ClusterSpec|cluster_spec_silver|1004
2016|Silver_AccountLogins|accountlogin|1004
2017|Silver_ModifiedAccounts|account_mods_silver|1004
2018|Silver_Notebooks|notebook_silver|1004
3001|Gold_Cluster|cluster_gold|2014
3002|Gold_Job|job_gold|2010
3003|Gold_JobRun|jobrun_gold|2011
3004|Gold_Notebook|notebook_gold|2018
3005|Gold_ClusterStateFact|clusterstatefact_gold|1005,2014
3007|Gold_AccountMod|account_mods_gold|2017
3008|Gold_AccountLogin|account_login_gold|2016
3010|Gold_SparkJob|sparkjob_gold|2006
3011|Gold_SparkStage|sparkstage_gold|2007
3012|Gold_SparkTask|sparktask_gold|2008
3013|Gold_SparkExecution|sparkexecution_gold|2005
3014|Gold_SparkExecutor|sparkexecutor_gold|2003
3015|Gold_jobRunCostPotentialFact|jobruncostpotentialfact_gold|3001,3003,3005,3010,3012



## Reloading Data
**CAUTION** Be very careful when attempting to reload bronze data, much of the bronze source data expires to 
minimize storage costs. Reloading bronze data should be a very rare task (hopefully never necessary)

The need may arise to reload data. This can be easily accomplished by setting the "Status" column to "ROLLED_BACK" 
for the modules you wish to reload where:
* fromTime column > some point in time OR
* Overwatch_RunID === some Overwatch_Run_ID (useful if duplicates get loaded)

Remember to specify the ModuleID for which you want to reload data.

### Reloading Data Example
**Scenario** Something has occurred and has corrupted the jobrun entity after 01-01-2021 but the issue wasn't 
identified until 02-02-2021. To fix this, all data from 12-31-2020 forward will be rolled back. Depending on the details,
the fromTime or the Pipeline_SnapTS could be used; for this scenario we will go back an extra day and use 
Pipeline_SnapTS as the incremental time column. Since we're using delta, we are safe to perform updates and deletes
without fear of losing data (In the event of an issue we could simply restore a previous version of the table). 

{{% notice warning%}}
[**Reloading spark silver**](#reloading-spark-data-silver) requires one extra step. If reloading spark silver data click the link. 
{{% /notice %}}

Steps:
1. Verify Overwatch isn't currently running and isn't about to kick off for this workspace.
2. Delete from jobRun_silver table where Pipeline_SnapTS >= "2020-12-31"
   
```scala
spark.sql("delete from overwatch_etl.jobRun_Silver where cast(Pipeline_SnapTS as date) >= '2020-12-31' ")
```

3. Delete from jobRun_gold table where Pipeline_SnapTS >= "2020-12-31"

```scala
spark.sql("delete from overwatch_etl.jobRun_Gold where cast(Pipeline_SnapTS as date) >= '2020-12-31' ")
```

4. Update the Pipeline_Report table set Status = "ROLLED_BACK" where moduleId in (2011,3003) and status in ('SUCCESS', 'EMPTY').
    * Modules 2011 and 3003 are the Silver and Gold modules for jobRun respectively
    * Only for Status values of 'SUCCESS' and 'EMPTY' because we want to maintain any 'FAILED' or 'ERROR' statuses that may be present from other runs

```scala
spark.sql(s"update overwatch_etl.pipeline_report set status = 'ROLLED_BACK' where moduleID in (2011,3003) and status in ('SUCCESS', 'EMPTY') ")
```

After performing the steps above, the next Overwatch run will load the data from 12-31 -> current.
{{% notice warning%}}
If back-loading more than 60 days, you must override the default primordialDateString in the config. Additionally, depending
on data volume and cluster size, it may be best to only load 60 days at a time using the *maxDaysToLoad* config. This
allows for the data to be loaded in chunks. Not necessary, but may have benefits depending on the situation.
{{% /notice %}}

{{% notice note%}}
For smaller, dimensional, silver/gold entities and testing, it's often easier to just drop the table than 
it is to delete records after some date. **Remember**, though, it's critical to set the primordialDateString from time if 
rebuilding entities with more than 60 days.
{{% /notice %}}

### Reloading Spark Data (Silver)
For performance reasons, there's one more update that must be made when reloading Spark Silver data. This data is 
derived from a VERY large events table, *overwatch_etl.spark_events_bronze*. To improve standard run performance,
new data that hasn't been processed is placed into it's own partition *Downstream_Processed = false"*. 

To set the *Downstream_Processed* flag to false for the correct records you must commit an update statement to the 
spark_events_bronze table as well before attempting to reload. This table is further partitions by "Event"; thus, the 
spark events to be reloaded can be further narrowed down with an Event filter. A map of spark entity to relevant 
events is provided below.

SparkGoldEntity             | In Clause
:---------------------------|:-------------------------------------------------------------
sparkExecutions             |('org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart', 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd')
sparkJob                    |('SparkListenerJobStart', 'SparkListenerJobEnd')
sparkStage                  |('SparkListenerStageSubmitted', 'SparkListenerStageCompleted')
sparkTask                   |('SparkListenerTaskStart', 'SparkListenerTaskEnd')
sparkExecutor               |('SparkListenerExecutorAdded', 'SparkListenerExecutorRemoved')

For another scenario, let's assume that the above steps have been completed but instead of jobRun, the sparkTasks
entity must be reloaded after 12-31. Unfortunately this table (spark_events_bronze) has not yet been partitioned by date
and the data volume for sparkTasks can be substantial. The best we can do in this scenario is to execute the statement
below and just wait. The more filters you can apply, the more efficient the reload will be, but don't be deceived, 
this is a raw table and many columns like "timestamp" you may expect to be populated often is not in all cases. Safe
filter columns include:
* Event (Must filter on this)
* Pipeline_SnapTS
* filenameGroup
* clusterId
* SparkContextId

```scala
val updateStatement =
  """
    |update overwatch_etl.spark_events_bronze set Downstream_Processed = false
    | where Event in ('SparkListenerTaskStart', 'SparkListenerTaskEnd')
    | and cast(Pipeline_SnapTS as date) >= '2020-12-31'
    |""".stripMargin
spark.sql(updateStatement)
```

### Running Only Specific Layers
As in the example above, it's common to need to rerun only a certain layer for testing or reloading data. In the 
example above it's inefficient to run the bronze layer repeatedly to reprocess two tables in silver and gold. If 
no new data is ingested into bronze, no modules outside of the two we rolled_back will have any new data and thus
will essentially be skipped resulting in the re-processing of only the two tables we're working on. 

The main class entrypoint for Overwatch runs all layers and there's no way around this. To run only specific layers
a notebook can be used. Build the config as normal, and then following the instructions in 
[Getting Started - Run Via Notebook]({{%relref "GettingStarted"%}}#run-via-notebook). This example illustrates how 
to run each of the three layers; Bronze, Silver, and Gold. In this scenario the Bronze(...) line would be commented
out to result in only a Silver/Gold layer run.

## Cloud-Specific Variations

