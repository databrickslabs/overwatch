---
title: "Running Overwatch"
date: 2022-12-12T11:37:52-05:00
weight: 3
---

An Overwatch deployment is simply a workload and as such can be run within a notebook or as a Job using a Main class

## As of Version 0.7.1.0
To deploy Overwatch through the following methods it is required to have version 0710+
* [Running Overwatch as a Notebook]({{%relref "DeployOverwatch/RunningOverwatch/Notebook.md"%}})
* [Running Overwatch as a JAR]({{%relref "DeployOverwatch/RunningOverwatch/JAR.md"%}})

## Legacy Deployments

{{% notice warning%}}
As of version 0.8.0.0 Overwatch is sunsetting legacy deployment methods. Please reference
the links above for a simpler and more advanced method for deploying Overwatch. 
{{% /notice %}}

## Migrating From Legacy Deployments (versions <0710)
If you're ready to migrate from the legacy deployment to the new deployment method, all you need to do is:
* Stop all the Overwatch jobs on all workspaces
* Follow the [deployment guide here]({{%relref "DeployOverwatch"%}})
* **BEFORE YOU RUN DEPLOYMENT** -- if you're using an internal metastore, execute the drop database commands below
    * This will drop your database schemas / metadata (NOT THE DATA) and ensure all the metadata paths line up. After 
    you complete the subsequent Overwatch run the databases and all the data will reappear in your metastore.
    * This will take your databases offline for a bit (on this workspace only) while the Overwatch job runs.
```sql
drop database <etl_database_name> cascade;
drop database <consumer_database_name> cascade;
```