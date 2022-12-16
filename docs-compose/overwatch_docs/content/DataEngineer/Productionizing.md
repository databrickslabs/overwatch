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