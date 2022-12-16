---
title: "Sharing Overwatch Data"
date: 2022-12-15T18:31:01-05:00
weight: 4
---

## Providing Access to Consumers
Now that Overwatch is deployed, it's likely that you want to share some or all of the data with its stakeholders.
We've made that easy to do across workspaces with a single function and it's parameters. This is a one time command 
that must be run on the workspace that wants to consumer the Overwatch Data.

### Registering Overwatch On Remote Workspaces

### Requirements

The customer who want to access the data have to run the below function.
* registerRemoteOverwatchIntoLocalMetastore

This function currently support below arguments:

Key | Type | Default Value | Description
:--------------------------|:---|:----------|:----------|:--------|:--------------------------------------------------
remoteStoragePrefix|String|NA|Remote storage prefix for the remote workspace where overwatch has been deployed
remoteWorkspaceID|String|NA|workSpaceID for the remoteworkspace where overwatch has been deployed. This will be used in getRemoteWorkspaceByPath()
localETLDatabaseName|String|""|ETLDatabaseName that user want to override. If not provided then by default etlDatabase name from the remoteWorkspace would be used as same for current workspace
localConsumerDatabaseName|String|""|ConsumerDatabase that user want to override. If not provided then by default ConsumerDatabase name from the remoteWorkspace would be used as same for current workspace
remoteETLDataPathPrefixOverride|String|""|Param to override StoragePrefix. If not provided then remoteStoragePrefix+"/global_share" would be used as StoragePrefix  
usingExternalMetastore|Boolean|false|Used in case if user using any ExternalMetastore.
workspacesAllowed|Array[String]|Array()|If we want to populate the table for a specific workSpaceID. In that case only ConsumerDB would be visible to the user.
 

### How it Works
Below is the steps on how the above function work:
* We have deployed Overwatch on one of our Workspace with WorkspaceID Workspace1 with the below params:
  
![Runner](/images/DeployOverwatch/Runner.png)

* Now if we want to deploy the tables from this workspace to a new workspace with WorkspaceID Workspace2.
  * Run the below command
```scala
Helpers.registerRemoteOverwatchIntoLocalMetastore(remoteStoragePrefix = "/mnt/overwatch_global/overwatch-5434",remoteWorkspaceID = "Workspace1",workspacesAllowed = Array())
```

* As we can see the tables are loaded in new workspace.
```sql
use overwatch_dev_5434;
show tables;
```
![Tables](/images/DeployOverwatch/Tables.png)
