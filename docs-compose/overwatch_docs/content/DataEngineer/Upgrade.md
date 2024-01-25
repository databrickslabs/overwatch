---
title: "Upgrades"
date: 2021-05-20T21:27:44-04:00
weight: 4
---

Sometimes upgrading from one version to the next requires a schema change. In these cases, the 
[CHANGELOG]({{%relref "ChangeLog/"%}}) will be explicit. Upgrades MUST be executed WITH the **new library (jar)** and 
**before the pipeline is executed**. 
Basic pseudocode can be found below as a reference. For actual version upgrade scripts please reference the upgrade 
scripts linked to your target version in the [Changelog]({{%relref "ChangeLog/"%}}).

{{% notice warning %}}
When a schema upgrade is required between versions, **this step cannot be skipped**. Overwatch will not allow you 
to continue on a version that requires a newer schema.
{{% /notice %}}

A sample notebook is provided below for reference.

[Upgrade.html](/assets/ChangeLog/Upgrade_Example.html) / [Upgrade.dbc](/assets/ChangeLog/Upgrade_Example.dbc)