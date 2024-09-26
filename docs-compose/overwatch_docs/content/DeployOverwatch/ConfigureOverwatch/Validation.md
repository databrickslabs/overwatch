---
title: "Configuration Validation"
date: 2022-12-12T11:36:53-05:00
weight: 4
---

Many validations are performed to minimize mistakes. The following section offers details on all validations
done. Not all validation steps are done on all runs. All validations should be performed before a first run on any
given workspace. For more information on executing a full validation, see [below]()

### Validation rules

| Name                                             | Validation Rule                                                                                                                                                                                                                                        | Impacted columns                                                                                     |
|:-------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------|
| Api Url Validation                               | API URL should give some response with provided scope and key.                                                                                                                                                                                         | api_url                                                                                              |
| Primordial data validation                       | Primordial Date must be in **yyyy-MM-dd format** (i.e. 2022-01-30 == Jan 30, 2022) and must be less than current date.                                                                                                                                 | primordial_date                                                                                      |
| Excluded scope validation                        | Excluded scope must be null or in colon-delimited format and include only the following audit:sparkEvents:jobs:clusters:clusterEvents:notebooks:pools:accounts:dbsql                                                                                   | excluded_scopes                                                                                      |
| Event Hub **Shared Access Key** (Azure Only)     | Audit Log data must be recognized as present and readable from the provided Event Hub Configuration fields.                                                                                                                                            | eh_name, eh_scope_key, secret_scope                                                                  |
| Event Hub **AAD Auth** (Azure Only)              | Audit Log data must be recognized as present and readable from the provided Event Hub Configuration fields.                                                                                                                                            | eh_name, eh_conn_string, aad_tenant_id, aad_client_id, aad_client_secret_key, aad_authority_endpoint |
| Common consumer database name                    | All workspaces must have a common consumer database name.                                                                                                                                                                                              | consumer_database_name                                                                               |
| Common ETL database name                         | All workspaces must have a common ETL database name.                                                                                                                                                                                                   | etl_database_name                                                                                    |
| Storage prefix validation                        | All workspaces must share a single storage prefix and the Overwatch cluster must have appropriate access to read/write.                                                                                                                                | storage_prefix                                                                                       |
| Cloud provider validation                        | Either Azure or AWS.                                                                                                                                                                                                                                   | cloud                                                                                                |
| Max days validation                              | Must Be a Number                                                                                                                                                                                                                                       | max_days                                                                                             |
| Secrete scope validation                         | Secret scope must not be empty.                                                                                                                                                                                                                        | secret_scope                                                                                         |
| PAT key validation                               | DBPAT must not be empty and must be able to authenticate to the workspace.                                                                                                                                                                             | secret_key_dbpat                                                                                     |
| Audit log location validation **(AWS/GCP ONLY)** | Audit logs must present immediately within the provided path and must be read accessible                                                                                                                                                               | auditlogprefix_source_path                                                                           |
| Mount point validation                           | Workspaces with more than 50 mount points need to provide a csv file which will contain the mount point to source mapping. **[click here for more details]({{%relref "DataEngineer/AdvancedTopics"%}}/#exception---remote-workspaces-with-50-mounts)** | mount_mapping_path                                                                                   |
| System Table Validation                          | System table should enabled and must have data for the workspace.                                                                                                                                                                                      | auditlogprefix_source_path                                                                           |

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
