---
title: "Validation"
date: 2022-12-12T11:36:53-05:00
weight: 4
---
## This section will walk you through the steps necessary to validate the Config.csv.
### What is validation
The config csv should be validated before performing the deployment.Below are some validations that will be perform in this process

### Validation rules

Name | Validation Rule | Impacted columns
:------|:----------------|:--------------------------
Api Url Validation| API URL should give some response with provided scope and key. |api_url
Primordial data validation|Primordial Date should in yyyy-MM-dd format(Ex:2022-01-30) and should be less than current date.|primordial_date
Excluded scope validation|Excluded scope can be audit:sparkEvents:jobs:clusters:clusterEvents:notebooks:pools:accounts:dbsql.|excluded_scopes
Consuming data from event hub(Azure)| Consuming data from event hub with provided ehName,scope and eh_key.|eh_name,eh_scope_key,secret_scope
Common consumer database name|Workspaces should have a common consumer database name.|consumer_database_name
Common ETL database name|Workspaces should have a common ETL database name.|etl_database_name
ETL storage prefix validation|ETL storage prefix should be common for all the workspaces and should have read,write and create access.|etl_storage_prefix
Cloud provider validation|Cloud provider should be either Azure or AWS.|cloud
Max days validation|Max days should be a number.|max_days
Secrete scope validation|Secrete scope schold not be empty.|secret_scope
PAT key validation| Dbpat should not be empty.|secret_key_dbpat
Audit log location validation(AWS)|Audit log should be present with read permission.|auditlogprefix_source_aws
Mount point validation|Each workspace should not have more than 50 mount points.|




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