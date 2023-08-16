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
#### (This will be Deprecated from version 7.2.1.Please refer Snapshot for backup process)
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

With 0.7.2.1 release this snapshot would be done by Snapshot Class.

Currently, There are two types of Snapshot process available in Overwatch:

**Batch Snapshot**

**Incremental Snapshot** 

#### Batch Snaphot
This is normal Snapshot process where we take the backup of Bronze in Batch method. Here we can do Deep Cloning of existing Bronze table to target Snapshot RootPath.

#### Incremental Snap
Through Incremental_Snap we take the snapshot of the bronze table incrementally i.e. through streaming operation.
As described before as this is a streaming process we have to maintain a checkpoint directory. Below is the screenshot of the snap location after snapshot process is done:

1. "snapshotRootPath/data" - Contains the backup data of Bronze Tables.
2. "snapshotRootPath/clone_report" - Contains the data containing run metrics.
3. "snapshotRootPath/checkpoint" - Checkpoint Location for Streaming Operation.

#### How to run the Snapshot Process
Currently, Users need to run the snapshot process through 
1. Databricks Job.
2. Notebook.

##### Snapshot through Databricks Notebook

The code snippet to run the Snapshot process is as below:

```scala
import com.databricks.labs.overwatch.pipeline.Snapshot
val sourceETLDB = "ow_bronze_snapshot_etl"
val targetPrefix = "/mnt/overwatch_playground/721-snap/incremental"
val pipeline = "Bronze,Silver,Gold"
val snapshotType = "Incremental"
val tablesToExclude = "notebook_silver,notebook_gold"
Snapshot.process(sourceETLDB,targetPrefix,snapshotType)
```

We need below parameters to run the snapshot process through Databricks Notebook:

| Param           | Type   | Optional | Default Value        | Description                                                                                                                                        |
|-----------------|--------|----------|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| sourceETLDB     | String | No       | NA                   | Source Database Name for which Snapshot need to be done.                                                                                           |
| targetPrefix    | String | No       | NA                   | Target path where Snapshot need to be done.                                                                                                        |
| snapshotType    | String | No       | NA                   | Type of Snapshot to be performed. "Full" for Full Snapshot, "Incremental" for Incremental Snapshot                                                 |
| pipeline        | String | Yes      | "Bronze,Silver,Gold" | Define the Medallion Layers. Argument should be in form of "Bronze, Silver, Gold"(All 3 or any combination of them)                                |
| cloneLevel      | String | Yes      | "DEEP"               | Clone Level for Snapshot. By Default it is "DEEP". You can also specify "SHALLOW" Clone                                                            |
| tablesToExclude | String | Yes      | ""                   | Array of table names to exclude from the snapshot. Tables should be separated with ",". This is the table name only - without the database prefix. |

After snapshot process is done the tables from Source ETL Database will be cloned in the target prefix location. The structure for target prefix location would be like below:

![Snapshot_Target](/images/DataEngineer/snapshotTarget.png)

All the tables data would be there in "targetPrefix/data" folder as shown below:

![Snapshot_Target_Data](/images/DataEngineer/snapshotTarget_Data.png)

If you look into the targetPrefix folder there would one another subdirectory called "clone_report". This folder contains the data regarding the report to show the status of your snapshot process for each table:

![clone_report](/images/DataEngineer/clone_report.png)

##### Snapshot through Databricks Job

Snapshot process can also be configured using Databricks Job. For this we need to run SnapshotRunner Class.

Below is the screenshot of the Job through which we run the snapshot process.

1. **Type** - Jar
2. **Main class** - com.databricks.labs.overwatch.pipeline.SnapshotRunner
3. **Parameters for the Job** - Same as mentioned above for `Snapshot through Databricks Notebook`

![Snapshot_Job](/images/DataEngineer/snapshot_job.png)


#### Validation Functionality
This is a special module in Snapshot Process. By default, it is part of Snapshot.process() but if user want to run it separately they can run it separately 
to check whether the configuration they would provide for Snapshot process, are correct or not.

Below is the code snippet for the validation function

```scala
import com.databricks.labs.overwatch.pipeline.Snapshot
val sourceETLDB = "ow_bronze_snapshot_etl"
val pipeline = "Bronze,Silver,Gold"
val snapshotType = "Incremental"
Snapshot.isValid(sourceETLDB,snapshotType,pipeline)
```

Upon running the above function you will get proper output whether the input parameters you would be using for Snapshot process,passed the validation check or not. One example of the same is as below:

```
ow_bronze_snapshot_etl is Overwatch Database and suitable for Snapshot
Snapshot Type is Suitable for Snapshot Process. Provided SnapshotType value is Incremental
Zone should be either Bronze,Silver or Gold. Provided Zone value is Bronze
Zone should be either Bronze,Silver or Gold. Provided Zone value is Silver
Zone should be either Bronze,Silver or Gold. Provided Zone value is Gold
cloneLevel Type is Suitable for Snapshot Process. Provided cloneLevel value is DEEP
Validation successful.You Can proceed with Snapshot process
import com.databricks.labs.overwatch.pipeline.Snapshot
res14: Boolean = true
```

### Restore Process
Restore is the reverse process of Snapshot.In data world the term Restore is very common. It simply means if our current working space is corrupted then we can restore the current workspace from the snapshot location we have used earlier.

Same like Snapshot Restore can be run by both Databricks Job and Databricks Notebook.

##### Restore through Databricks Notebook

The code snippet to run the Restore process is as below:
```scala
import com.databricks.labs.overwatch.pipeline.Restore
val sourcePrefix = "/mnt/overwatch_playground/721-snap/full"
val targetPrefix = "/mnt/overwatch_playground/721-snap/restore"
Restore.process(sourcePrefix,targetPrefix)
```
We need below parameters to run the Restore process through Databricks Notebook:

| Param          | Type   | Optional  | Description                                                                              |
|----------------|--------|-----------|------------------------------------------------------------------------------------------|
| sourcePrefix   | String | No        | Source ETL Path Prefix from where restore need to be performed.                          |
| targetPrefix   | String | No        | Target ETL Path Prefix to where restore data would be loaded.                            |

After Restore process is done the tables from sourcePrefix will be restored in the `target prefix/globalshare` location

![Restore](/images/DataEngineer/restore_path.png)

##### Restore through Databricks Job
Restore process can also be configured using Databricks Job. For this we need to run RestoreRunner Class.

Below is the screenshot of the Job through which we run the snapshot process.

1. **Type** - Jar
2. **Main class** - com.databricks.labs.overwatch.pipeline.RestoreRunner
3. **Parameters for the Job** - Same as mentioned above for `Restore through Databricks Notebook`

![Restore_Job](/images/DataEngineer/restore_job.png)

#### Validation Functionality
This is a special module in Restore  Process. By default, it is part of Restore.process() but if user want to run it separately they can run it separately
to check whether the configuration they would provide for Restore process, are correct or not.

Below is the code snippet for the validation function

```scala
import com.databricks.labs.overwatch.pipeline.Restore
val sourcePrefix = "/mnt/overwatch_playground/721-snap/full"
val targetPrefix = "/mnt/overwatch_playground/721-snap/restore"
Restore.isValid(sourcePrefix,targetPrefix)
```

Upon running the above function you will get proper output whether the input parameters you would be using for Restore process,passed the validation check or not. One example of the same is as below:

```
SourcePrefix Path Exists.
Target Path /mnt/overwatch_playground/721-snap/restore is Empty and is Suitable for Restore Process
Validation successful.You Can proceed with Restoration process
import com.databricks.labs.overwatch.pipeline.Restore
res43: Boolean = true
```


### Migration Process
Migration Process is same as Snapshot process with some added Functionality. Below are the steps involved in Migration Process:

1. Stop Overwatch jobs  (Need to be done by User)
2. Migrate all old data to new location. (Would be done by OW Migration Process) 
3. Update Config with new storage location (Would be done by OW Migration Process)
4. Delete Old Database  (Would be done by OW Migration Process)
5. Resume Overwatch jobs (Need to be done by User)

##### Migration through Databricks Notebook
The code snippet to run the Migration process is as below:
```scala
import com.databricks.labs.overwatch.pipeline.Migration
val sourceETLDB = "ow_bronze_migration_etl"
val migrateRootPath = "/mnt/overwatch_playground/721-snap/migration"
val configPath = "abfss://overwatch-field-playground@overwatchglobalinternal.dfs.core.windows.net/sourav/configs/721-migration"
val tablesToExclude = "notebook_silver:notebook_gold"
Migration.process(sourceETLDB,migrateRootPath,configPath)
```
We need below parameters to run the Migration process through Databricks Notebook:

| Param           | Type     | Optional   | Description                                                                                                                           |
|-----------------|----------|------------|---------------------------------------------------------------------------------------------------------------------------------------|
| sourceETLDB     | String   | No         | Source Database Name for which Migration need to be done.                                                                             |
| migrateRootPath | String   | No         | Target path to where migration need to be performed.                                                                                  |
| configPath      | String   | No         | Configuration Path where the config file for the source is present. Path can be CSV file or delta path ot delta table.                |
| tablesToExclude | String   | Yes        | Array of table names to exclude from the snapshot. This is the table name only - without the database prefix. By Default it is empty. |

After Migration process is done the tables from sourcePrefix will be restored in the `migrateRootPath/globalshare` location
![Migration](/images/DataEngineer/Migration_path.png)


##### Migration through Databricks Job
Below is the Configuration for the Databricks Job
1. **Type** - Jar
2. **Main class** - com.databricks.labs.overwatch.pipeline.MigrationRunner
3. **Parameters for the Job** Same as mentioned above for `Migration through Databricks Notebook`


![Migration_Job](/images/DataEngineer/Migration_Job.png)

#### Validation Functionality
This is a special module in Migration Process. By default, it is part of Migration.process() but if user want to run it separately they can run it separately
to check whether the configuration they would provide for Migration process, are correct or not.

Below is the code snippet for the validation function

```scala
import com.databricks.labs.overwatch.pipeline.Migration
val sourceETLDB = "ow_bronze_migration_etl"
val configPath = "abfss://overwatch-field-playground@overwatchglobalinternal.dfs.core.windows.net/sourav/configs/721-migration"
Migration.isValid(sourceETLDB,configPath)
```
Upon running the above function you will get proper output whether the input parameters you would be using for Migration process,passed the validation check or not. One example of the same is as below:

```
ow_bronze_migration_etl is Overwatch Database and suitable for Migration
Config file is properly configured and suitable for Migration
Config source: delta path abfss://overwatch-field-playground@overwatchglobalinternal.dfs.core.windows.net/sourav/configs/721-migration
Validation successful.You Can proceed with Migration process
import com.databricks.labs.overwatch.pipeline.Migration
res13: Boolean = true
```



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