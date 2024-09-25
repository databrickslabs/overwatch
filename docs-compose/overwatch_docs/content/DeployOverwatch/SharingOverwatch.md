---
title: "Sharing Overwatch Data"
date: 2022-12-15T18:31:01-05:00
weight: 5
---

## Granting Access Via DBSQL
If your storage prefix is referencing a mount point, nothing should be necessary here as the mount point is already 
accessible to all workspace users. If using a direct path (i.e. s3://, abfss://, or gs://) you will need to configure 
the appropriate access in Admin Settings --> SQL Warehouse Settings.

If using a **Hive Metastore** you may still need to review your grants to ensure users have access to the Overwatch 
tables / views. Additionally, you may need to grant select on any file to the appropriate user groups if direct path 
is used for overwatch instead of a mount point.
```GRANT SELECT ON ANY FILE TO `<user>@<domain-name>` ```

If using **Unity Catalog** be sure to provision the appropriate access to the user groups on the Overwatch Schemas.

## Providing Access to Consumers On Remote Workspaces
Now that Overwatch is deployed, it's likely that you want to share some or all of the data with stakeholders.
We've made that easy to do across workspaces with a single function and it's parameters 
`Helpers.registerRemoteOverwatchIntoLocalMetastore`. This is a one time command 
that must be run on the workspace that wants to consumer the Overwatch Data.

{{% notice note %}}
This is only necessary when using multi-workspace deployment and not using a shared metastore.
{{% /notice %}}

### Registering Overwatch On Remote Workspaces
The command below is the simplest version and will publish the etl and consumer databases with all the data to the 
workspace on which the command is run. Note, this must be run on the target workspace, not the deployment workspace.

The following are just a few basic examples, please refer to the [optional parameters](#function-argument-details) 
below for a full list of options and details that can be passed to the function.

As an example let's say we have two workspaces. Overwatch was deployed on workspace 123 (i.e. 123 is the workspace ID) 
and is configured to monitor workspaces 123 and 234. After initial deployment users on workspace 234 will not be able to 
see the databases. If you wish for the users on 234 to be able to see the database\[s\] simply run the function below 
based on your needs

**You want users of workspace 234 to see all Overwatch data**

Run the following command on workspace 234 and the ETL and Consumer databases and all tables and all data will appear 
in the metastore for workspace 234.
```scala
import com.databricks.labs.overwatch.utils.Helpers
Helpers.registerRemoteOverwatchIntoLocalMetastore(remoteStoragePrefix = "/mnt/overwatch_global/overwatch-5434", remoteWorkspaceID = "123")
```

**You want users of workspace 234 to see only their data**

If you only want workspace 234 users to see their data (i.e. from workspace 234 not 123) then run the following 
command. This will register only the consumer database and only the records that pertain to workspace 234. The ETL 
database will not be published.
```scala
import com.databricks.labs.overwatch.utils.Helpers
Helpers.registerRemoteOverwatchIntoLocalMetastore(remoteStoragePrefix = "/mnt/overwatch_global/overwatch-5434", remoteWorkspaceID = "123", workspacesAllowed = Array("234"))
```

**You want users of workspace 234 to see Overwatch data for several workspaces**

If there were many workspaces and you wanted to share data from multiple workspaces it's the same command just add to 
the additional workspaceIDs to the "workspacesAllowed" Array. Obviously this would require Overwatch to be configured 
for more workspaces but this is meant to extend the examples above.
```scala
import com.databricks.labs.overwatch.utils.Helpers
Helpers.registerRemoteOverwatchIntoLocalMetastore(remoteStoragePrefix = "/mnt/overwatch_global/overwatch-5434", remoteWorkspaceID = "123", workspacesAllowed = Array("234", "345", "456"))
```


### Requirements
* **Overwatch JAR** - The cluster used to run the commands above on workspace 234 must have Overwatch JAR 0.7.1.0+ attached to the cluster.
* **Security** - Any compute that will be reading the data must have storage access to read the storage prefix, in the example 
  above, "/mnt/overwatch_global/overwatch-5434". If the path is mounted on workspace 234, this access will be available 
  to all compute in the workspace. If only certain clusters should have access the cluster must be configured with 
  an Instance Profile (AWS) or 
  [SPN permissions (AZURE)]({{%relref "DeployOverwatch/ConfigureOverwatch/SecurityConsiderations.md"%}}/#azure-storage-auth-config)
* **Database Names** - The local database names don't already exist. Unless overridden, these names will be the same as on workspace 123.

{{% notice note %}}
This feature will not work with legacy deployments. The databases and views are registered by default during the 
Overwatch pipeline run. Consider upgrading if you would like to use this feature.
{{% /notice %}}

### Function Argument Details

This function currently support below arguments:

Key | Type | Default Value | Description
:--------------------------|:---|:----------|:----------|:--------|:--------------------------------------------------
remoteStoragePrefix|String|NA|Remote storage prefix for the remote workspace where overwatch has been deployed
remoteWorkspaceID|String|NA|workSpaceID for the remoteworkspace where overwatch has been deployed. This will be used in getRemoteWorkspaceByPath()
localETLDatabaseName|String|Same name as the ETL database on the deployment workspace|ETLDatabaseName that user want to override. If not provided then by default etlDatabase name from the remoteWorkspace would be used as same for current workspace
localConsumerDatabaseName|String|Same name as the Consumer database on the deployment workspace|ConsumerDatabase that user want to override. If not provided then by default ConsumerDatabase name from the remoteWorkspace would be used as same for current workspace
remoteETLDataPathPrefixOverride|String|""|Param to override StoragePrefix. If not provided then remoteStoragePrefix+"/global_share" would be used as StoragePrefix  
usingExternalMetastore|Boolean|false|Used in case if user using any ExternalMetastore.
workspacesAllowed|Array[String]|Array()|If we want to populate the table for a specific workSpaceID. In that case only ConsumerDB would be visible to the user.

### How it Works
The consumer database is just a database full of views, so when you pass in a list of workspaceIds into 
the "workspacesAllowed" parameter, it registers

### Example With Screenshots
Below are the details of how the function works:
* We have deployed Overwatch on one of our workspaces (workspace with id 123) with the below params:
  
![Runner](/images/DeployOverwatch/Runner.png)

* Now if we want to deploy the tables from this workspace to a new workspace, say workspaceID 234 to continue our 
  previous example.
  * Run the below command on workspace 234
```scala
Helpers.registerRemoteOverwatchIntoLocalMetastore(remoteStoragePrefix = "/mnt/overwatch_global/overwatch-5434",remoteWorkspaceID = "123", workspacesAllowed = Array())
```

* As we can see the tables are loaded in new workspace.
```sql
-- from workspace 234
use overwatch_dev_5434;
show tables;
```
![Tables](/images/DeployOverwatch/Tables.png)
