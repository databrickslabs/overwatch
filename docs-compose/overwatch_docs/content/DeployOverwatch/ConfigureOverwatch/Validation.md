---
title: "Validation"
date: 2022-12-12T11:36:53-05:00
weight: 4
---
## Deployment Validations
Many validations are performed to minimize mistakes. The following section offers details on all validations 
done. Not all validation steps are done on all runs. All validations should be performed before a first run on any 
given workspace. For more information on executing a full validation, see [below]()

### Validation rules

Name | Validation Rule                                                                                                                                                      | Impacted columns
:------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------
Api Url Validation| API URL should give some response with provided scope and key.                                                                                                       |api_url
Primordial data validation| Primordial Date must be in **yyyy-MM-dd format** (i.e. 2022-01-30 == Jan 30, 2022) and must be less than current date.                                               |primordial_date
Excluded scope validation| Excluded scope must be null or in colon-delimited format and include only the following audit:sparkEvents:jobs:clusters:clusterEvents:notebooks:pools:accounts:dbsql |excluded_scopes
Consuming data from event hub(Azure)| Audit Log data must be recognized as present and readable from the provided Event Hub Configuration fields.                                                          |eh_name,eh_scope_key,secret_scope
Common consumer database name| All workspaces must have a common consumer database name.                                                                                                            |consumer_database_name
Common ETL database name| All workspaces must have a common ETL database name.                                                                                                                 |etl_database_name
Storage prefix validation| All workspaces must share a single storage prefix and the Overwatch cluster must have appropriate access to read/write.                                              |storage_prefix
Cloud provider validation| Either Azure or AWS.                                                                                                                                                 |cloud
Max days validation| Must Be a Number                                                                                                                                                     |max_days
Secrete scope validation| Secret scope must not be empty.                                                                                                                                      |secret_scope
PAT key validation| DBPAT must not be empty and must be able to authenticate to the workspace.                                                                                           |secret_key_dbpat
Audit log location validation **(AWS/GCP ONLY)**| Audit logs must present immediately within the provided path and must be read accessible                                                                             |auditlogprefix_source_path
Mount point validation[ *More Details](#mount-point-limitation)| Workspaces with more than 50 mount points are not supported in remote deployments at this time.                                                                      |

### Run the validation

```scala
import com.databricks.labs.overwatch.MultiWorkspaceDeployment
val configCsvPath = "dbfs:/FileStore/overwatch/workspaceConfig.csv" // Path to the config.csv

// temp location which will be used as a temp storage. It will be automatically cleaned after each run.
val tempLocation = "/tmp/overwatch/templocation"

// number of workspaces to validate in parallel. Exceeding 20 may require larger drivers or additional cluster config considerations
// If total workspaces <= 20 recommend setting parallelism == to workspace count 
val parallelism = 4

// Run validation
MultiWorkspaceDeployment(configCsvPath, tempLocation)
  .validate(parallelism)
```

### Review The Report
The validation report will be generated in <etl_storage_prefix>/report/validationReport as delta table.
Run the below query to check the validation report. All records should say validated = true or action is necessary prior 
to deployment.

**NOTE** The validation report maintains a full history of validations. If you've validated multiple times be sure 
to look at the snapTS and only review the validations relevant for the latest validation run.

```sql
-- full report
select * from delta.`<etl_storage_prefix>/report/validationReport`
order by snapTS desc
```

```sql
-- only validation error
select * from delta.`<etl_storage_prefix>/report/validationReport` where validated = 'false'
order by snapTS desc
```

```scala
display(
  spark.read.format("delta").load("<etl_storage_prefix>/report/validationReport")
    .orderBy('snapTS.desc)
)
```

### Mount Point Limitation
As you can see from the validation, monitoring remote workspaces is not supported for workspaces with > 50 mount points. 
This is because the Databricks API that provides a map of mount points to fully-qualified URIs returns only 50 results 
and does not support pagination. Databricks is aware of this limitation and is working to prioritize pagination. In 
the meantime the dev team is looking for work-arounds and will publish updates as soon as a viable alternative can be 
found.

**If Using AWS And All Cluster Logs are going to s3:// direct paths**

Currently, workspaces with more than 50 mount points must have a local Overwatch deployment due to this limitation. 
Deactivate / Remove the workspace in question from the config and add the workspace to a new config where it is the 
only configured workspace. Create a local Overwatch deployment for that one deployment local to that workspace.