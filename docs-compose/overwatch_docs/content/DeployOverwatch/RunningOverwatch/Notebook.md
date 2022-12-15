---
title: "As A Notebook"
date: 2022-12-12T12:04:26-05:00
weight: 2
---
## This section will walk you through the steps necessary to deploy Overwatch through Notebook.

### Step-1: Provide config.csv path

```scala
val configCsvPath = "dbfs:/FileStore/overwatch/workspaceConfig.csv" //Provide the path of the config.csv
```

### Step-2: Initialise MultiworkspaceDeployment

```scala

val multiWorkspaceDeployment = com.databricks.labs.overwatch.MultiWorkspaceDeployment(configCsvPath,"/mnt/tmp/overwatch/templocation") // Path /mnt/tmp/overwatch/templocation is a temp location which will be used as a temp storage.It will be automatically cleaned after each run.

```

### Step-3: Run the deployment
```scala
multiWorkspaceDeployment.deploy(2,"Bronze,Silver,Gold")
//Args(0) is the number of threads it will be using to perform deployment.
//Args(1) is the Zone for which the deployment will be performed, if you want to perform deployment for a particular zone then it also can be given as
// ex: multiWorkspaceDeployment.deploy(2,"Bronze")
//     multiWorkspaceDeployment.deploy(2,"Silver")
//     multiWorkspaceDeployment.deploy(2,"Gold")
```
### Checking the Status of the deployment
The deployment report will be generated in /<etl_storage_prefix>/report/deploymentReport as delta table.
Run the below query to check the deployment report.
```roomsql
%sql
select * from delta.`/<etl_storage_prefix>/report/deploymentReport`
```
