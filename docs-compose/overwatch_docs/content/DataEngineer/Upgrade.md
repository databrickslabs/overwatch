---
title: "Upgrade"
date: 2021-05-20T21:27:44-04:00
weight: 3
---

Sometimes upgrading from one version to the next requires a schema change. In these cases, the 
[CHANGELOG]({{%relref "ChangeLog/"%}}) will be explicit. The general upgrade process is:
* Import the upgrade package
* Retrieve your production workspace
    * Should be done with the same parameters and method as the pipeline / environment that is being upgraded
* Call the upgrade function for the version to which you're upgrading.

To upgrade to version 0.4.12, the code is below. Note, the "buildWorkspace" function is just a helper function to 
construct the workspace. Same as using Databricks widgets and passing parameters, this function just 
builds the OverwatchParams and returns the workspace instance.

{{% notice warning %}}
When a schema upgrade is required between versions, **this step cannot be skipped**. Overwatch will not allow you 
to continue on a version that requires a newer schema.
{{% /notice %}}

```scala
import com.databricks.labs.overwatch.utils.Upgrade
val prodConfigs = Map(
  "storagePrefix" -> "...",
  "etlDBName" -> "overwatch_etl",
  "consumerDBName" -> "overwatch",
  "secretsScope" -> "...",
  "dbPATKey" -> "...",
  "ehKey" -> "...",
  "ehName" -> "...",
  "primordialDateString" -> "2021-01-01",
  "maxDaysToLoad" -> "60",
  "scopes" -> "all"
)

val logger: Logger = Logger.getLogger("Upgrade")
val parallelism = 24
val prodWorkspace = buildWorkspace(prodConfigs)

val upgradeReport = Upgrade.upgrade0412(prodWorkspace)
upgradeReport.show()
```
