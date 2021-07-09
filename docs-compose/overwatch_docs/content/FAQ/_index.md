---
title: FAQ
date: 2021-01-12T05:49:17-05:00
weight: 5
---

## Using the FAQs
The answers and examples to the questions below assume that a workspace is instantiated and ready to use. 
There are many ways to instantiate a workspace, below is a simple way but it doesn't matter how you create the 
Workspace, just so long as the state of the workspace and/or pipeline can be referenced and utilized. 
A simple example is below. There are much more verbose details in the 
[Advanced Topics Section]({{%relref "GettingStarted/AdvancedTopics.md"%}}/#interacting-with-overwatch-and-its-state) 
if you'd like a deeper explanation.
```scala
import com.databricks.labs.overwatch.utils.Upgrade
import com.databricks.labs.overwatch.pipeline.Initializer
val params = OverwatchParams(...)
val prodArgs = JsonUtils.objToJson(params).compactString

// A more verbose example is available in the example notebooks referenced above
val prodWorkspace = Initializer(prodArgs, debugFlag = true)
val upgradeReport = Upgrade.upgradeTo042(prodWorkspace)
display(upgradeReport)
```

**Q 1: I just deployed Overwatch and made a mistake and just want to clean up everything and re-deploy. I can't 
seem to get a clean state from which to begin though.**

**A 1**: Overwatch tables are intentionally created as external tables. When the 
[etlDataPathPrefix]({{%relref "GettingStarted/Configuration.md"%}}/#datatarget) is configured (as recommended) 
the target data does not live underneath the database directory and as such is not deleted when the database is dropped. 
This is considered an "external table" and this is done by design to protect the org from someone accidentally dropping 
all data globally. For a FULL cleanup you may use the functions below but these are destructive, be sure that 
you review it and fully understand it before you run it.

{{% notice warning %}}
**CRITICAL**: **CHOOSE THE CORRECT FUNCTION FOR YOUR ENVIRONMENT**
If Overwatch is configured on multiple workspaces, use the multi-workspace script otherwise, use the 
single workspace functions. Please do not remove the safeguards as they are there to protect you and your company from 
accidents.
{{% /notice %}}

**SINGLE WORKSPACE DEPLOYMENT DESTROY AND REBUILD**

[single_workspace_cleanup.dbc](/assets/FAQ/single_workspace_cleanup.dbc) | 
[single_workspace_cleanup.html](/assets/FAQ/single_workspace_cleanup.html)

**MULTI WORKSPACE DEPLOYMENT DESTROY AND REBUILD**

First off, don't practice deploy a workspace to the global Overwatch data target as it's challenging to clean-up; 
nonetheless, the script below will remove the data from this workspace from the global dataset and reset this 
workspace for a clean run.

[multi_workspace_cleanup.dbc](/assets/FAQ/multi_workspace_cleanup.dbc) |
[multi_workspace_cleanup.html](/assets/FAQ/multi_workspace_cleanup.html)

**Q 2: The incorrect costs were entered for a specific time range, I'd like to correct them, how should I do that?**

**A 2**: Please refer to the [Configuring Custom Costs]({{%relref "GettingStarted"%}}#configuring-custom-costs) 
to baseline your understanding. Also, closely review the details of the 
[instanceDetails]({{%relref "DataEngineer/Definitions.md"%}}/#instancedetails)
table definition as that's where costs are stored and this is what will need to be accurate. Lastly, 
please refer to the [ETL_Process]({{%relref "DataEngineer/ETL_Process.md"%}}) section of the docs as 
you'll likely need to understand how to move the pipeline through time.

Now that you have a good handle on the costs and the process for restoring / reloading data, note that there are 
ultimately two options after you reconfigure the instancedetails table to be accurate. There are two tables that 
are going to have inaccurate data in them: 
* jobruncostpotentialfact_gold
* clusterstatefact

**Options**
1. Completely drop tables, delete their underlying data and rebuild them from the primordial date
2. Delete all records where Pipeline_SnapTS >= the time where the data became inaccurate.

Both options will require a corresponding appropriate update to the pipeline_report table for the two modules 
that write to those tables (3005, 3015). [Here are several examples]({{%relref "DataEngineer/ETL_Process.md"%}}/#reloading-data-example)
on ways to update the pipeline_report to change update the state of a module.