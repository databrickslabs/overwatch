---
title: "Upgrades"
date: 2021-05-20T21:27:44-04:00
weight: 4
---

Sometimes upgrading from one version to the next requires a schema change. In these cases, the 
[CHANGELOG]({{%relref "ChangeLog/"%}}) will be explicit. Upgrades MUST be executed WITH the **new library (jar)** and 
**before the pipeline is executed**. The general upgrade process is:
* Use the compactString of parameters to instantiate the workspace
  * The compact string can be found in your original runner notebook which you got from 
    [here]({{%relref "DeployOverwatch/RunningOverwatch/NotebookLegacy"%}}#jump-start-notebooks)
* Call the upgrade function for the version to which you're upgrading and pass in the workspace object

Basic pseudocode can be found below as a reference. For actual version upgrade scripts please reference the upgrade 
scripts linked to your target version in the [Changelog]({{%relref "ChangeLog/"%}}).

{{% notice warning %}}
When a schema upgrade is required between versions, **this step cannot be skipped**. Overwatch will not allow you 
to continue on a version that requires a newer schema.
{{% /notice %}}

A sample notebook is provided below for reference.

[Upgrade.html](/assets/ChangeLog/Upgrade_Example.html) / [Upgrade.dbc](/assets/ChangeLog/Upgrade_Example.dbc)

```scala
import com.databricks.labs.overwatch.utils.Upgrade
import com.databricks.labs.overwatch.pipeline.Initializer
val prodArgs = """<myConfigCompactString>""" // from runner notebook

// A more verbose example is available in the example notebooks referenced above
val prodWorkspace = Initializer(prodArgs)
val upgradeReport = Upgrade.upgradeTo060(prodWorkspace, ...)
display(upgradeReport)
Upgrade.finalize060Upgrade("<overwatch_etl_db_name>")
```
