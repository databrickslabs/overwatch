---
title: "Upgrade"
date: 2021-05-20T21:27:44-04:00
weight: 3
---

Sometimes upgrading from one version to the next requires a schema change. In these cases, the 
[CHANGELOG]({{%relref "ChangeLog/"%}}) will be explicit. Upgrades MUST be executed WITH the **new binary** and 
**before the pipeline is executed**. The general upgrade process is:
* Adjust any Overwatch configurations and get the updated compactString from JsonUtils
* Import the upgrade package
* Use the compactString of parameters to instantiate the workspace
* Call the upgrade function for the version to which you're upgrading.

To upgrade to version 0.4.12, the code is below. Note, the "buildWorkspace" function is just a helper function to 
construct the workspace. Same as using Databricks widgets and passing parameters, this function just 
builds the OverwatchParams and returns the workspace instance.

{{% notice warning %}}
When a schema upgrade is required between versions, **this step cannot be skipped**. Overwatch will not allow you 
to continue on a version that requires a newer schema.
{{% /notice %}}

A sample notebook is provided below for reference.

[Upgrade.html](/assets/ChangeLog/Upgrade_Example.html) / [Upgrade.dbc](/assets/ChangeLog/Upgrade_Example.dbc)

```scala
import com.databricks.labs.overwatch.utils.Upgrade
import com.databricks.labs.overwatch.pipeline.Initializer
val params = OverwatchParams(...)
val prodArgs = JsonUtils.objToJson(params).compactString

// A more in-depth example is available in the example notebooks referenced above
val prodWorkspace = Initializer(prodArgs, debugFlag = true)
val upgradeReport = Upgrade.upgradeTo042(prodWorkspace)
display(upgradeReport)
```
