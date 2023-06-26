---
title: "Productionizing"
date: 2022-07-20T15:03:23-04:00
weight: 5
---

## Moving To Production
When you're ready to move to production, there are a few things to keep in mind and best practices to follow to 
get the most out of Overwatch

### Cluster Logging
**Simplify and Unify your cluster logging directories**
* Many users forget to enable cluster logging and without it Overwatch cannot provide usage telemetry by notebook,
  job, user so it's critical that all clusters have clusters logs enabled
* If users are allowed to create clusters/jobs without any governance, log files will be produced and stored all 
over the place. These will be very challenging to clean up and can eat up a significant amount of storage over time.
* Utilize cluster policies to ensure the logging location is always set and done so consistently
* Set up a lifecycle policy (i.e. TTL) on your cluster logging directory in your cloud storage so the logs don't 
pile up indefinitely. Suggested time to live time is 30 days.

### Backups
#### (This will be Deprecated in Future Release.Please refer Snapshot in Future)
**Perform Bronze Backups**
I know we don't hear a lot about backups in big data world but often times the cluster logs and / or 
the audit logs are transient (especially Azure deployments as Event Hub only maintains 7 days). This means that if
something happened to the bronze data your Overwatch history could be lost forever. To guard against this it's
strongly recommended that you periodically backup the Bronze data. As of 0.6.1.1 this has been made very easy through
the `snapshot` helper function.

Now just schedule a notebook like this to run once a week and you'll always have at 1 week of backups

```scala
import com.databricks.labs.overwatch.pipeline.Bronze
import com.databricks.labs.overwatch.utils.Helpers
val workspace = Helpers.getWorkspaceByDatabase("overwatch_etl") // can be used after pipeline is running successfully

// alternative method for getting a workspace is 
// import com.databricks.labs.overwatch.pipeline.Initializer
// val workspace = Initializer("""<compact config string>""")

// simple method
Bronze(workspace).snapshot("/path/to/my/backups/sourceFolder", true)

// alternative method with more customized configs
Bronze(workspace).snapshot(
  targetPrefix = "/path/to/my/backups/sourceFolder",
  overwrite = true,
  excludes = Array("audit_log_raw_events", "clusters_snapshot_bronze") // not recommended but here for example purposes
)
```

#### Overwrite Option
Note that the snapshot function performs a deep clone for all bronze tables except the tables in the excludes
array. As such, these are complete backups each time. If the *overwrite* option is set to true you will have a backup
created each run but it will be replaced on each subsequent run. If you would like to maintain more than one backup 
you must set *overwrite = false*. If you set overwrite to false, you may want to consider a cleanup function to ensure 
you don't have 52 copies of bronze per year (assuming 1X / week).

### Snapshot Process
Snapshoting in Big Data World is very rare but often times the cluster logs and / or
the audit logs are transient (especially Azure deployments as Event Hub only maintains 7 days). This means that if
something happened to the bronze data your Overwatch history could be lost forever. To guard against this it's
strongly recommended that you periodically snapshot the Overwatch data.

With 0.7.x.x release this snapshot would be done by Snapshot Class.

Currently, There are two types of Snapshot process available in Overwatch:

**Batch Snapshot**

**Incremental Snapshot(Support Streaming Operation)**

#### How to run the Snapshot Process
Currently, Users need to run the snapshot process through 
1. Databricks Job.
2. Through Notebook.

##### Snapshot through Databricks Job
Below is the Configuration for the Databricks Job

Below is the screenshot of the Job through which we run the snapshot process.

1. **Type** - Jar
2. **Main class** - com.databricks.labs.overwatch.pipeline.Snapshot
3. **Parameters for the Job** 

| Param  | Type   | Optional | Description                                                                                |
|--------|--------|----------|--------------------------------------------------------------------------------------------|
|SourceETLDB | String | No       | Source Database Name for which Snapshot need to be done.                                |
| snapshotRootPath | String | No       | Target path where Snapshot need to be done.                                        |
| pipeline | String | No       | Define the Medallion Layers. Argumnent should be in form of "Bronze, Silver, Gold"(All 3 or any combination of them)                                                        |
| snapshotType | String | No       | Type of Snapshot to be performed. "Full" for Full Snapshot , "Incremental" for Incremental Snapshot |
| tablesToExclude | String | Yes      | Array of table names to exclude from the snapshot. This is the table name only - without the database prefix. By Default it is empty. |

![Snapshot_Job](/images/DataEngineer/Snapshot_Job.png)

##### Snapshot through Databricks Notebook
```scala
import com.databricks.labs.overwatch.pipeline.Snapshot
val sourceETLDB = "ow_bronze_snapshot_etl"
val snapshotRootPath = "/mnt/overwatch_playground/721-snap/incremental"
val pipeline = "Bronze:Silver:Gold"
val snapshotType = "Incremental"  // "Full" For Batch Snapshot
val tablesToExclude = "notebook_silver:notebook_gold"
Snapshot.main(Array(sourceETLDB,snapshotRootPath,pipeline,snapshotType,tablesToExclude))

```

### Restore Process
Restore is the reverse process of Snapshot.In data world the term Restore is very common. It simply means if our current working space is corrupted then we can restore the current workspace from the snapshot location we have used earlier.

Same like Snapshot Restore can be run by both Databricks Job and Databricks Notebook.

##### Restore through Databricks Job
Below is the Configuration for the Databricks Job
1. **Type** - Jar
2. **Main class** - com.databricks.labs.overwatch.pipeline.Restore
3. **Parameters for the Job**

| Param  | Type   | Optional | Description                                                                                  |
|--------|--------|----------|----------------------------------------------------------------------------------------------|
|sourcePrefix | String | No       | Source ETL Path Prefix from where restore need to be performed.                         |
|targetPrefix | String | No       | Target ETL Path Prefix to where restore data would be loaded.                           |

![Restore_Job](/images/DataEngineer/Restore.png)

##### Restore through Databricks Notebook
```scala
import com.databricks.labs.overwatch.pipeline.Restore
val sourcePrefix = "/mnt/overwatch_playground/721-snap/full"
val targetPrefix = "/mnt/overwatch_playground/721-snap/restore"
Restore.main(Array(sourcePrefix,targetPrefix))
```

### Migration Process
Migration Process is same as Snapshot process with some added Functionality. Below are the steps involved in Migration Process:

1. Stop Overwatch jobs  (Need to be done by User)
2. Migrate all old data to new location. (Done by OW Migration Job) 
3. Update Config with new storage location (Done by OW Migration Job)
4. Delete Old Database  (Done by OW Migration Job)
5. Restart jobs (Need to be done by User)

##### Migration through Databricks Job
Below is the Configuration for the Databricks Job
1. **Type** - Jar
2. **Main class** - com.databricks.labs.overwatch.pipeline.Migration
3. **Parameters for the Job**

| Param  | Type   | Optional | Description                                                                                  |
|--------|--------|----------|----------------------------------------------------------------------------------------------|
|sourceETLDB | String | No       | Source Database name or ETlDataPathPrefix name.                         |
|migrateRootPath | String | No       | Target path to where migration need to be performed.                           |
|configPath | String | No       | Configuration Path where the config file for the source is present. Path can be CSV file or delta path ot delta table.                           |
|tablesToExclude | String | Yes      | Array of table names to exclude from the snapshot. This is the table name only - without the database prefix. By Default it is empty.                           |

![Migration_Job](/images/DataEngineer/Migration.png)


```scala
import com.databricks.labs.overwatch.pipeline.Migration
val sourceETLDB = "ow_bronze_migration_etl"
val migrateRootPath = "/mnt/overwatch_playground/721-snap/migration"
val configPath = "abfss://overwatch-field-playground@overwatchglobalinternal.dfs.core.windows.net/sourav/configs/721-migration"
val tablesToExclude = "notebook_silver:notebook_gold"
Migration.main(Array(sourceETLDB,migrateRootPath,configPath,tablesToExclude))
```



##### Migration through Databricks Notebook

#### Batch Snaphot
This is normal Snapshot process where we take the backup of Bronze in Batch method. Here we can do Deep Cloning of existing Bronze table to target Snapshot RootPath

#### Incremental Snap
Through Incremental_Snap we take the snapshot of the bronze table incrementally i.e. through streaming operation.
As described before as this is a streaming process we have to maintain a checkpoint directory. Below is the screenshot of the snap location after snapshot process is done:

1. "snapshotRootPath/data" - Contains the backup data of Bronze Tables
2. "snapshotRootPath/clone_Report" - Contains the data containing run metrics
3. "snapshotRootPath/checkpoint" - Checkpoint Location for Streaming Operation

### Alerting On Failures
Overwatch modules are designed to fail softly. This means that if your silver jobs module fails the job will still 
succeed but the silver jobs module will not progress and neither will any downstream modules that depend on it. To 
be alerted when a specific module fails you need to configure a DBSQL alert to monitor the pipReport output and 
fire an alert when conditions are met.

#### How To Set Up Module Level Alerting
Documentation in progress -- publishing soon

### Ensure Externalize Optimize is Enabled
To ensure Overwatch is efficient it's important to remove the optimize and z-order steps from the integrated pipeline. 
To do this, follow the instructions for 
[Externalizing Optimize & Z-Order]({{%relref "DataEngineer/AdvancedTopics"%}}#externalize-optimize--z-order-as-of-060) 