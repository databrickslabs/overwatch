---
title: "Validation"
date: 2022-12-12T11:36:53-05:00
draft: true
weight: 2
---
## This section will walk you through the steps necessary to validate the Config.csv.
### What is validation
The config csv should be validated before performing the deployment.Below are some validations that will be perform in this process
* Validate API URL should.
* Validate secret Scope.
* Validate Event Hub connection(Azure).
* Validate S3 Audit log location(AWS).
* Validate number of mount points. Mount point numbers in a workspace should not exceed 50(Azure).
* Validate read/write access.
* Validate primordial date.
* Validate max date.
* Validate ETL storage prefix(It should be common for all the workspaces)
* Validate consumer/etl database name(It should be common for all the workspaces)

### Perform the below steps to validate the Config.csv
#### Step-1: Provide config.csv path

```scala
val configCsvPath = "dbfs:/FileStore/overwatch/workspaceConfig.csv" //Provide the path of the config.csv
```

#### Step-2: Initialise MultiworkspaceDeployment

```scala

val multiWorkspaceDeployment = com.databricks.labs.overwatch.MultiWorkspaceDeployment(configCsvPath,"/mnt/tmp/overwatch/templocation") // Path /mnt/tmp/overwatch/templocation is a temp location which will be used as a temp storage.It will be automatically cleaned after each run.

```

#### Step-3: Run the validation
```scala
 multiWorkspaceDeployment.validate(4)
//Args(0) is the number of threads it will be using to perform the validation.

```
#### Step-4: Check the validation report
The validation report will be generated in /<etl_storage_prefix>/report/validationReport as delta table.
Run the below query to check the validation report. 
```roomsql
%sql
select * from delta.`/<etl_storage_prefix>/report/validationReport` where validated='false'
```
The query should return 0 results. Check validationMsg column in case of failed validations and modify the Config.csv accordingly.