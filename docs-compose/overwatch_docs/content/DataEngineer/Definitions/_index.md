---
title: "Data Dictionary (Latest)"
date: 2021-01-11T12:21:19-05:00
weight: 1
---

## ERD
The "ERD" below is a visual representation of the [consumer layer](#consumption-layer-tables-views) data model. 
Many of the joinable lines have been omitted to reduce 
chaos and complexity in the visualization. All columns with the same name are joinable (even if there's not a line 
from one table to the other). The relations depicted are to call the analyst's attention to less obvious joins.

The goal is to present a data model that unifies the different parts of the platform. The Overwatch team will continue 
to work with Databricks platform teams to publish and simplify this data. The gray boxes annotated as 
"Backlog/Research" are simply a known gap and a pursuit of the Overwatch dev team, it does NOT mean it's going to be 
released soon but rather that we are aware of the missing component and we hope to enable gold-level data here in 
the future.

![OverwatchERD](/images/_index/Overwatch_Gold_070.png)

## Consumption Layer "Tables" (Views)
All end users should be hitting consumer tables first. Digging into lower layers gets significantly more complex.
Below is the data model for the consumption layer. The consumption layer is often in a stand-alone database apart
from the ETL tables to minimize clutter and confusion. These entities in this layer are actually not tables at all
(with a few minor exceptions such as lookup tables) but rather views. This allows for the Overwatch development team
to alter the underlying columns, names, types, and structures without breaking existing transformations. Instead, 
view column names will remain the same but may be repointed to a newer version of a column, etc. 

{{% notice note %}}
ETL should not be developed atop the consumption layer views but rather the gold layer. Before Overwatch version 
upgrades, it's important that the engineering team review the change list and upgrade requirements before upgrading.
These upgrades may require a remap depending on the changes. As of version 1.0 release, all columns in the gold layer
will be underscored with their schema version number, column changes will reference the later release version but the 
views published with Overwatch will almost always point to the latest version of each column and will not include the
schema suffix to simplify the data model for the average consumer.
{{% /notice %}}

### Data Organization
The large gray boxes in the simplified ERD below depict the two major, logical sections of the data model:
* **Databricks Platform** - Metadata captured by the Databricks platform that can be used to assist in workspace
  governance. This data can also be enriched with the Spark data enabling in-depth analyses. The breadth of metadata
  is continuing to grow, stay tuned for additional capabilities.
* **Spark UI** The spark UI section is derived from the spark event logs and essentially contains every single piece
  of data from the Spark UI. There are a few sections that are not included in the first release but the data is 
  present in *spark_events_bronze* albeit extremely complex to derive. The Overwatch development team is working 
  tirelessly to expose additional SparkUI data and will publish as soon as it's ready.

### Column Descriptions
Complete column descriptions are only provided for the consumption layer. The entity names are linked below.

* [cluster](#cluster)
* [clusterStateFact](#clusterstatefact)
* [instanceDetails](#instancedetails)
* [job](#job)
* [jobrun](#jobrun)
* [jobRunCostPotentialFact](#jobruncostpotentialfact)
* [sqlQueryHistory](#sqlqueryhistory)
* [notebook](#notebook)
* [instancePool](#instancepool)
* [dbuCostDetail](#dbucostdetails)
* [accountLogin](#accountlogin)
* [accountMod](#accountmod)
* [sparkExecution](#sparkexecution)
* [sparkExecutor](#sparkexecutor)
* [sparkJob](#sparkjob)
* [sparkStage](#sparkstage)
* [sparkTask](#sparktask)
* [sparkStream](#sparkstream_preview) **preview
* [Common Meta Fields](#common-meta-fields)
  * There are several fields that are present in all tables. Instead of cluttering each table with them, this section
  was created as a reference to each of these.

{{% notice info %}}
Most tables below provide a data **SAMPLE** for reference. You may either click to view it or 
right click the SAMPLE link and click saveTargetAs or saveLinkAs and save the file.
Note that these files are **TAB** delimited, so you will need to view as such if you save to local file. 
The data in the files were generated from an Azure, test deployment created by Overwatch Developers.
{{% /notice %}}

#### Cluster
[**SAMPLE**](/assets/TableSamples/cluster.tab)

**KEY** -- organization_id + cluster_id + unixTimeMS

**Incremental Columns** -- unixTimeMS

**Partition Columns** -- organization_id

**Write Mode** -- Append

| Column                    | Type          | Description                                                                                                                                                                                                                                                                                              |
|:--------------------------|:--------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cluster_id                | string        | Canonical Databricks cluster ID (more info in [Common Meta Fields](#common-meta-fields))                                                                                                                                                                                                                 |
| action                    | string        | create, edit, or snapImpute -- depicts the type of action for the cluster -- **snapImpute is used on first run to initialize the state of the cluster even if it wasn't created/edited since audit logs began                                                                                            |
| timestamp                 | timestamp     | timestamp the action took place                                                                                                                                                                                                                                                                          |
| cluster_name              | string        | user-defined name of the cluster                                                                                                                                                                                                                                                                         |
| driver_node_type          | string        | Canonical name of the driver node type.                                                                                                                                                                                                                                                                  |
| node_type                 | string        | Canonical name of the worker node type.                                                                                                                                                                                                                                                                  |
| num_workers               | int           | The number of workers defined WHEN autoscaling is disabled                                                                                                                                                                                                                                               |
| autoscale                 | struct        | The min/max workers defined WHEN autoscaling is enabled                                                                                                                                                                                                                                                  |
| auto_termination_minutes  | int           | The number of minutes before the cluster auto-terminates due to inactivity                                                                                                                                                                                                                               |
| enable_elastic_disk       | boolean       | Whether autoscaling disk was enabled or not                                                                                                                                                                                                                                                              |
| is_automated              | booelan       | Whether the cluster is automated (true if automated false if interactive)                                                                                                                                                                                                                                |
| cluster_type              | string        | Type of cluster (i.e. Serverless, SQL Analytics, Single Node, Standard)                                                                                                                                                                                                                                  |
| security_profile          | struct        | Complex type to describe secrity features enabled on the cluster. More information [Below]()                                                                                                                                                                                                             |
| cluster_log_conf          | string        | Logging directory if configured                                                                                                                                                                                                                                                                          |
| init_script               | array<struct> | Array of init scripts                                                                                                                                                                                                                                                                                    |
| custom_tags               | string        | User-Defined tags AND also includes Databricks JobID and Databricks RunName when the cluster is created by a Databricks Job as an automated cluster. Other Databricks services that create clusters also store unique information here such as SqlEndpointID when a cluster is created by "SqlAnalytics" |
| cluster_source            | string        | Shows the source of the action **(TODO -- checking on why null scenario with BUI)                                                                                                                                                                                                                        |
| spark_env_vars            | string        | Spark environment variables defined on the cluster                                                                                                                                                                                                                                                       |
| spark_conf                | string        | custom spark configuration on the cluster that deviate from default                                                                                                                                                                                                                                      |
| acl_path_prefix           | string        | Automated jobs pass acl to clusters via a path format, the path is defined here                                                                                                                                                                                                                          |
| instance_pool_id          | string        | Canononical pool id from which workers receive nodes                                                                                                                                                                                                                                                     |
| driver_instance_pool_id   | string        | Canononical pool id from which driver receives node                                                                                                                                                                                                                                                      |
| instance_pool_name        | string        | Name of pool from which workers receive nodes                                                                                                                                                                                                                                                            |
| driver_instance_pool_name | string        | Name of pool from which driver receives node                                                                                                                                                                                                                                                             |
| spark_version             | string        | DBR version - scala version                                                                                                                                                                                                                                                                              |
| idempotency_token         | string        | Idempotent jobs token if used                                                                                                                                                                                                                                                                            |

#### ClusterStateFact
[**SAMPLE**](/assets/TableSamples/clusterstatefact.tab)

**KEY** -- organization_id + cluster_id + state + unixTimeMS_state_start

**Incremental Columns** -- state_start_date + unixTimeMS

**Partition Columns** -- organization_id + state_start_date

**Z-Order Columns** -- cluster_id + unixTimeMS_state_start

**Write Mode** -- Merge

##### Unsupported Scenarios
A few scenarios are not yet supported by Overwatch; they are called out here. Please stay tuned for updates
as it's our intention to include everything we can as soon as possible after Databricks product GAs new features but
there will be a delay.
* Costs for DBSQL clusters
  * Even though DBSQL warehouses may show up here (they normally will not), the costs for these cannot be calculated 
  at this time. Databricks doesn't yet publish the warehouse event logs (i.e. start/stop/scale) and until that is
  available, we cannot estimate costs for warehouses like we do for traditional clusters.

Costs and state details by cluster at every state in the cluster lifecycle. 
The **[Cost Functions](#cost-functions-explained) are detailed below** the definitions of this table.

{{% notice warning %}}
Any static clusters spanning 90 days without any state changes will never get a state closure and result in costs 
increasing forever. This should be a VERY rare circumstance and usually only happens in extreemely stable, small 
streams. This max days for clsf will be externalized as an override config in the future but for now it's static.
{{% /notice %}}

{{% notice note %}}
This fact table is **not normalized on time**. Some states will span multiple days and must be smoothed across 
days (i.e. divide by days_in_state) when trying to calculate costs by day. All states are force-terminated at the 
end of the Overwatch run to the until-timestamp of the run. If the state was still active at this time, it will be 
updated on the subsequent run.
{{% /notice %}}

| Column                  | Type        | Description                                                                                                                                                    |
|:------------------------|:------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cluster_id              | string      | Canonical Databricks cluster ID (more info in [Common Meta Fields]())                                                                                          |
| cluster_name            | string      | Name of cluster at beginning of state                                                                                                                          |
| custom_tags             | string      | JSON string of key/value pairs for all cluster associated custom tags give to the cluster                                                                      |
| \*_state_start          | various     | timestamp reference column at the time the state began                                                                                                         |
| \*_state_end            | various     | timestamp reference column at the time the state ended                                                                                                         |
| state                   | string      | state of the cluster -- full list [HERE](https://docs.databricks.com/dev-tools/api/latest/clusters.html#clustereventtype)                                      |
| current_num_workers     | long        | number of workers in use by the cluster at the start of the state                                                                                              |
| target_num_worers       | long        | number of workers targeted to be present by the completion of the state. Should be equal to *current_num_workers* except during RESIZING state                 |
| uptime_since_restart_S  | double      | Seconds since the cluster was last restarted / terminated                                                                                                      |
| uptime_in_state_S       | double      | Seconds the cluster spent in current state                                                                                                                     |
| uptime_in_state_H       | double      | Hours the cluster spent in current state                                                                                                                       |
| driver_node_type_id     | string      | KEY of driver node type to enable join to [instanceDetails](#instanceDetails)                                                                                  |
| node_type_id            | string      | KEY of worker node type to enable join to [instanceDetails](#instanceDetails)                                                                                  |
| cloud_billable          | boolean     | All current known states are cloud billable. This means that cloud provider charges are present during this state                                              |
| databricks_billable     | boolean     | State incurs databricks DBU costs. All states incur DBU costs except: INIT_SCRIPTS_FINISHED, INIT_SCRIPTS_STARTED, STARTING, TERMINATING, CREATING, RESTARTING |
| isAutomated             | boolean     | Whether the cluster was created as an "automated" or "interactive" cluster                                                                                     |
| dbu_rate                | double      | Effective dbu rate used for calculations (effective at time of pipeline run)                                                                                   |
| runtime_engine          | string      | One of STANDARD or PHOTON. When PHOTON, pricing is adjusted when deriving the dbu_costs                                                                        |
| state_dates             | array<date> | Array of all dates across which the state spanned                                                                                                              |
| days_in_state           | int         | Number of days in state                                                                                                                                        |
| worker_potential_core_H | double      | Worker core hours available to execute spark tasks                                                                                                             |
| core_hours              | double      | All core hours of entire cluster (including driver). Nodes * cores * hours in state                                                                            |
| driver_compute_cost     | double      | Compute costs associated with driver runtime                                                                                                                   |
| driver_dbu_cost         | double      | DBU costs associated with driver runtime                                                                                                                       |
| worker_compute_cost     | double      | Compute costs associated with worker runtime                                                                                                                   |
| worker_dbu_cost         | double      | DBU costs associated with cumulative runtime of all worker nodes                                                                                               |
| total_driver_cost       | double      | Driver costs including DBUs and compute                                                                                                                        |
| total_worker_cost       | double      | Worker costs including DBUs and compute                                                                                                                        |
| total_compute_cost      | double      | All compute costs for Driver and Workers                                                                                                                       |
| total_dbu_cost          | double      | All dbu costs for Driver and Workers                                                                                                                           |
| total_cost              | double      | Total cost from Compute and DBUs for all nodes (including Driver)                                                                                              |
| driverSpecs             | struct      | Driver node details                                                                                                                                            |
| workerSpecs             | struct      | Worker node details                                                                                                                                            |

##### Cost Functions Explained
**EXPECTATIONS** -- Note that Overwatch costs are derived. This is good and bad. Good as it allows for costs to be 
broken down by any dimension at the millisecond level. Bad because there can be significant differences between the 
derived costs and actual costs. These should generally be very close to equal but may differ within margin of error by 
as much as 10%. To verify the cost functions and the elements therein feel free to review them in more detail. If 
your costs are off by a large marine, please review all the components of the cost function and correct any configurations 
as necessary to align your reality with the Overwatch config. The default costs are list price and often do not 
accurately reflect a customer's costs.
* **driver_compute_cost**: when cloudBillable --> Driver Node Compute Contract Price Hourly (instanceDetails) * Uptime_In_State_H --> otherwise 0
* **worker_compute_cost**: when cloudBillable --> Worker Node Compute Contract Price Hourly (instanceDetails) * Uptime_In_State_H * target_num_workers --> otherwise 0
  * target_num_workers used here is ambiguous. Assuming all targeted workers can be provisioned, the calculation is most accurate; 
  however, if some workers cannot be provisioned the worker_compute_cost will be slightly higher than actual while
  target_num_workers > current_num_workers. target_num_workers used here because the compute costs begin accumulating 
  as soon as the node is provisioned, not at the time it is added to the cluster.
* **photon_kicker**: when runtime_engine == Photon 2 otherwise 1
* **driver_dbu_cost**: when databricks_billable --> driver_hourly_dbus (instancedetails.hourlyDBUs) * houry_dbu_rate for dbu type (dbuCostDetails.contract_price) *
  uptime_in_state_H * photon_kicker --> otherwise 0
* **worker_dbu_cost**: when databricks_billable --> driver_hourly_dbus (instancedetails.hourlyDBUs) * houry_dbu_rate for dbu type (dbuCostDetails.contract_price) *
  current_num_workers * uptime_in_state_H * photon_kicker --> otherwise 0
  * current_num_workers used here as dbu costs do not begin until the node able to receive workloads (i.e. node is 
    moved from target_worker to current_worker / "upsize_complete" state)
* **cloudBillable**: Cluster is in a running state
  * GAP: Note that cloud billable ends at the time the cluster is terminated even though the nodes remain provisioned 
    in the cloud provider for several more minutes; these additional minutes are not accounted for in this 
    cost function.


#### InstanceDetails
[**AWS Sample**](/assets/TableSamples/instancedetails_aws.tab) | [**AZURE_Sample**](/assets/TableSamples/instancedetails_azure.tab)

**KEY** -- Organization_ID + API_name

**Incremental Columns** -- Pipeline_SnapTS

**Partition Columns** -- organization_id

**Write Mode** -- Append


This table is unique and it's purpose is to enable users to identify node specific contract costs associated with 
Databricks and the Cloud Provider through time. 
Defaults are loaded as an example by workspace. These defaults are meant to be reasonable, not accurate by default 
as there is a wide difference between cloud discount rates and prices between regions / countries. 
Everytime Overwatch runs, it validates the presence of 
this table and whether it has any data present for the current workspace, if it does not it creates and appends the relevant
data to it; otherwise no action is taken. This gives the user the ability to extend / customize this table to fit their 
needs by workspace. Each organization_id (workspace), should provide complete cost data for each node used in that
workspace. If you decide to completely customize the table, it's critical to note that **some columns are required** 
for the ETL to function; these fields are indicated below in the table with an asterisk.

The organization_id (i.e. workspace id) is automatically generated for each workspace if that organization_id is not
present in the table already (or the table is not present at all). Each workspace
(i.e. organization_id) often has unique costs, this table enables you to customize compute pricing.

**IMPORTANT** This table must be configured such that there are no overlapping costs (by time) and no gaps (by time) 
in costs for any key (organization_id + API_name) between primordial date and current date. 
This means that for a record to be "expired" the following must be true:
* original key expired by setting activeUntil == expiry date
* original key must be created with updated information and must:
  * have activeFrom == expiry date of previous record (no gap, no overlap)
  * have activeUntil == lit(null).cast("date")

[Azure VM Pricing Page](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)

[AWS EC2 Pricing Page](https://aws.amazon.com/ec2/pricing/on-demand/)

| Column                     | Type    | Description                                                                                                                                                                                                                                                         |
|:---------------------------|:--------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| instance                   | string  | Common name of instance type                                                                                                                                                                                                                                        |
| API_name*                  | string  | Canonical KEY name of the node type -- use this to join to node_ids elsewhere                                                                                                                                                                                       |
| vCPUs*                     | int     | Number of virtual cpus provisioned for the node type                                                                                                                                                                                                                |
| Memory_GB                  | double  | Gigabyes of memory provisioned for the node type                                                                                                                                                                                                                    |
| Compute_Contract_Price*    | double  | Contract price for the instance type as negotiated between customer and cloud vendor. This is the value used in cost functions to deliver cost estimates. It is defaulted to equal the on_demand compute price                                                      |
| On_Demand_Cost_Hourly      | double  | On demand, list price for node type DISCLAIMER -- cloud provider pricing is dynamic and this is meant as an initial reference. This value should be validated and updated to reflect actual pricing                                                                 |
| Linux_Reserved_Cost_Hourly | double  | Reserved, list price for node type DISCLAIMER -- cloud provider pricing is dynamic and this is meant as an initial reference. This value should be validated and updated to reflect actual pricing                                                                  |
| Hourly_DBUs*               | double  | Number of DBUs charged for the node type                                                                                                                                                                                                                            |
| is_active                  | boolean | whether the contract price is currently active. This must be true for each key where activeUntil is null                                                                                                                                                            |
| activeFrom*                | date    | The start date for the costs in this record. **NOTE** this MUST be equal to one other record's activeUntil unless this is the first record for these costs. There may be no overlap in time or gaps in time.                                                        |
| activeUntil*               | date    | The end date for the costs in this record. Must be null to indicate the active record. Only one record can be active at all times. The key (API_name) must have zero gaps and zero overlaps from the Overwatch primordial date until now indicated by null (active) |

#### dbuCostDetails
**KEY** -- Organization_ID + sku

**Incremental Columns** -- activeFrom

**Partition Columns** -- organization_id

**Write Mode** -- Append

Slow-changing dimension to track DBU contract costs by workspace through time. This table should only need to be edited 
in very rare circumstances such as historical cost correction. Note that editing these contract prices will not 
retroactively modify historical pricing in the costing table such as clusterStateFact or jobRunCostPotentialFact. For 
prices to be recalculated, the gold pipeline modules must be rolled back properly such that the costs can be 
rebuilt with the updated values.

| Column         | Type    | Description                                                                                                                                                                                                                                                         |
|:---------------|:--------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sku            | string  | One of automated, interactive, jobsLight, sqlCompute                                                                                                                                                                                                                |
| contract_price | double  | Price paid per DBU on the sku                                                                                                                                                                                                                                       |
| is_active      | boolean | whether the contract price is currently active. This must be true for each key where activeUntil is null                                                                                                                                                            |
| activeFrom*    | date    | The start date for the costs in this record. **NOTE** this MUST be equal to one other record's activeUntil unless this is the first record for these costs. There may be no overlap in time or gaps in time.                                                        |
| activeUntil*   | date    | The end date for the costs in this record. Must be null to indicate the active record. Only one record can be active at all times. The key (API_name) must have zero gaps and zero overlaps from the Overwatch primordial date until now indicated by null (active) |

#### Job
[**SAMPLE**](/assets/TableSamples/job.tab)

The below columns closely mirror the APIs listed below by action. For more details about these fields and 
their structures please reference the relevant Databricks Documentation for the action.

Note -- Databricks has moved to API2.1 for all jobs-related functions and in-turn, Databricks has moved 
several fields from the root level to a nested level to support multi-task jobs. These 
root level fields are still visible in Overwatch as some customers are still using legacy APIs and many customers have 
historical data by which this data was generated using the legacy 2.0 APIs. These fields can be identified by the 
prefix, "Legacy" in the Description and have been colored red on the ERD.

| Action       | API                                                                                                                                                                                                                                                                                                                   |
|:-------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SnapImpute   | Only created during the first Overwatch Run to initialize records of existing jobs not present in the audit logs. These jobs are still available in the UI but have not been modified since the collection of audit logs begun thus no events have been identified and therefore must be imputed to maximize coverage |
| Create       | ["Create New Job API"](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate)                                                                                                                                                                                                               |
| Update       | ["Partially Update a Job"](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsUpdate)                                                                                                                                                                                                           |
| Reset        | ["Overwrite All Settings for a Job"](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsReset)                                                                                                                                                                                                  |
| Delete       | ["Delete A Job"](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsDelete)                                                                                                                                                                                                                     |
| ChangeJobAcl | ["Update Job Permissions"](https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-job-permissions)                                                                                                                                                                                           |
| ResetJobAcls | ["Replace Job Permissions"](https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-job-permissions) -- Not yet supported                                                                                                                                                              |

**KEY** -- organization_id + job_id + unixTimeMS + action + request_id

**Incremental Columns** -- unixTimeMS

**Partition Columns** -- organization_id

**Write Mode** -- Append

| Column                    | Type      | Description                                                                                                                                                                                                                                                                                                                                                                       |
|:--------------------------|:----------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| organization_id           | string    | Canonical workspace id                                                                                                                                                                                                                                                                                                                                                            |
| workspace_name            | string    | Customer defined name of the workspace or workspace_id (default)                                                                                                                                                                                                                                                                                                                  |
| job_id                    | long      | Databricks job id                                                                                                                                                                                                                                                                                                                                                                 |
| action                    | string    | Action type defined by the record. One of: create, reset, update, delete, resetJobAcl, changeJobAcl. More information about these actions can be found [here](https://docs.databricks.com/dev-tools/api/latest/jobs.html)                                                                                                                                                         |
| date                      | date      | Date of the action for the key                                                                                                                                                                                                                                                                                                                                                    |
| timestamp                 | timestamp | Timestamp the action took place                                                                                                                                                                                                                                                                                                                                                   |
| job_name                  | string    | User defined name of job.                                                                                                                                                                                                                                                                                                                                                         |
| tags                      | map       | The tags applied to the job if they exist                                                                                                                                                                                                                                                                                                                                         |
| tasks                     | array     | The tasks defined for the job                                                                                                                                                                                                                                                                                                                                                     |
| job_clusters              | array     | The job clusters defined for the job                                                                                                                                                                                                                                                                                                                                              |
| libraries                 | array     | LEGACY -- Libraries defined in the job -- Nested within tasks as of API 2.1                                                                                                                                                                                                                                                                                                       |
| timeout_seconds           | string    | Job-level timeout seconds. Databricks supports timeout seconds at both the job level and the task level. Task level timeout_seconds can be found nested within tasks                                                                                                                                                                                                              |
| max_concurrent_runs       | long      | Job-level -- maximum concurrent executions of the job                                                                                                                                                                                                                                                                                                                             |
| max_retries               | long      | LEGACY -- Max retries for legacy jobs -- Nested within tasks as of API 2.1                                                                                                                                                                                                                                                                                                        |
| retry_on_timeout          | boolean   | LEGACY -- whether or not to retry if a job run times out -- Nested within tasks as of API 2.1                                                                                                                                                                                                                                                                                     |
| min_retry_interval_millis | long      | LEGACY -- Minimal interval in milliseconds between the start of the failed run and the subsequent retry run. The default behavior is that unsuccessful runs are immediately retried. -- Nested within tasks as of API 2.1                                                                                                                                                         |
| schedule                  | struct    | Schedule by which the job should execute and whether or not it is paused                                                                                                                                                                                                                                                                                                          |
| existing_cluster_id       | string    | LEGACY -- If compute is existing interactive cluster the cluster_id will be here -- Nested within tasks as of API 2.1                                                                                                                                                                                                                                                             |
| new_cluster               | struct    | LEGACY -- The cluster_spec identified as an automated cluster for legacy jobs -- Can be found nested within tasks now but **ONLY for direct API Calls, editing legacy jobs, AND sparkSumbit tasks (as they cannot use job_clusters)**, otherwise, new_clusters defined through the UI will be defined as "job_clusters" and referenced by a "job_cluster_key" in the tasks field. |
| git_source                | struct    | Specification for a remote repository containing the notebooks used by this job's notebook tasks.                                                                                                                                                                                                                                                                                 |
| task_detail_legacy        | struct    | LEGACY -- The job execution details used to be defined at the root level for API 2.0 as of API 2.1 they have been nested within tasks. The logic definition will be defined here for legacy jobs only (or new jobs created using the 2.0 jobs API)                                                                                                                                |
| is_from_dlt               | boolean   | Whether or not the job was created from DLT -- Unsupported as OW doesn't yet support DLT but left here as a reference in case it can be helpful                                                                                                                                                                                                                                   |
| aclPermissionSet          | struct    | Only populated for "ChangeJobAcl" actions. Defines the new ACLs for a job                                                                                                                                                                                                                                                                                                         |
| target_user_id            | string    | Databricks canonical user id to which the aclPermissionSet is to be applied                                                                                                                                                                                                                                                                                                       |
| session_id                | string    | session_id that requested the action                                                                                                                                                                                                                                                                                                                                              |
| request_id                | string    | request_id of the action                                                                                                                                                                                                                                                                                                                                                          |
| user_agent                | string    | request origin such as browser, terraform, api, etc.                                                                                                                                                                                                                                                                                                                              |
| response                  | struct    | response of api call including errorMessage, result, and statusCode (HTTP 200,400, etc)                                                                                                                                                                                                                                                                                           |
| source_ip_address         | string    | Origin IP of action requested                                                                                                                                                                                                                                                                                                                                                     |
| created_by                | string    | Email account that created the job                                                                                                                                                                                                                                                                                                                                                |
| created_ts                | long      | Timestamp the job was created                                                                                                                                                                                                                                                                                                                                                     |
| deleted_by                | string    | Email account that deleted the job -- will be null if job has not been deleted                                                                                                                                                                                                                                                                                                    |
| deleted_ts                | long      | Timestamp the job was deleted -- will be null if job has not been deleted                                                                                                                                                                                                                                                                                                         |
| last_edited_by            | string    | Email account that made the previous edit -- defaults to created by if no edits made                                                                                                                                                                                                                                                                                              |
| last_edited_ts            | long      | Timestamp the job was last edited                                                                                                                                                                                                                                                                                                                                                 |

#### JobRun
[**SAMPLE**](/assets/TableSamples/jobrun.tab)

Databricks has moved to "multi-task jobs" (MTJs) and each run now refers to the run of a task not a job. This migration 
will likely cause a lot of confusion so please read this carefully.

Each record references the full lifecycle of a single task run with some legacy fields to accommodate historical 
job-level runs (and jobs/runs still being created/launched from the deprecated Jobs 2.0 API). 
Since the inception of multi-task jobs and Databricks jobs API 2.1, all run logic has been migrated from 
the job-level to the task-level. Overwatch must accommodate both as many customers have historical data that is still 
important. As such, some of the fields seem redundant and the analyst must apply the correct logic based on the 
circumstances. Please carefully review the field descriptions to understand the rules.

##### Unsupported Scenarios
A few scenarios are not yet supported by Overwatch; they are called out here. Please stay tuned for updates 
as it's our intention to include everything we can as soon as possible after Databricks product GAs new features but 
there will be a delay.
* **DLT details** -- Delta Live tables aren't yet supported even though the run_ids may show up here
* **Runs that failed to launch** due to error in the launch request -- these never actually create a run and never receive a run_id therefore they will not be present in this table at this time.
* **Runs executing for more than 30 Days** -- This is a limitation for performance. This will be an externalized config at a later time but for now
the hard limit is 30 days. The progress of this feature can be tracked in [Issue 528](https://github.com/databrickslabs/overwatch/issues/528)

**TODO -- clarify the taskRunId vs jobRunId confusion from the UI**

**KEY** -- organization_id + run_id + startEpochMS

**Incremental Columns** -- startEpochMS

**Partition Columns** -- organization_id 

**Write Mode** -- Merge

Inventory of every canonical task run executed by databricks workspace.

| Column                    | Type    | Description                                                                                                                                                                                                                   |
|:--------------------------|:--------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| organization_id           | string  | Canonical workspace id                                                                                                                                                                                                        |
| workspace_name            | string  | Customer defined name of the workspace or workspace_id (default)                                                                                                                                                              |
| job_id                    | long    | ID of the job                                                                                                                                                                                                                 |
| job_name                  | string  | Name of the runName if run is named, otherwise it will be job name                                                                                                                                                            |
| job_trigger_type          | string  | One of "cron" (automated scheduled), "manual", "repair"                                                                                                                                                                       |
| terminal_state            | string  | State of the task run at the time of Overwatch pipeline execution                                                                                                                                                             |
| run_id                    | long    | The lowest level of the run_id (i.e. legacy jobs may not have a task_run_id, in this case, it will be the job_run_id).                                                                                                        |
| run_name                  | string  | The name of the run if the run is named (i.e. in submitRun) otherwise this is set == taskKey                                                                                                                                  |
| multitask_parent_run_id   | long    | If the task belongs to a multi-task job the job_run_id will be populated here, otherwise it will be null                                                                                                                      |
| job_run_id                | long    | The run id of the job, not the task                                                                                                                                                                                           |
| task_run_id               | long    | The run id of the task except for legacy and                                                                                                                                                                                  |
| repair_id                 | long    | If the task or job was repaired, the repair id will be present here and the details of the repair will be in repair_details                                                                                                   |
| task_key                  | string  | The name of the task is actually a key and must be unique within a job, this field specifies the task that was executed in this task_run_id                                                                                   |
| cluster_type              | string  | Type of cluster used in the execution, one of "new", "job_cluster", "existing", "SQL Warehouse", null -- will be null for DLT pipelines and/or in situations where the type is not provided from Databricks                   |
| cluster_id                | string  | The cluster ID of the compute used to execute the task run. If task executed on a SQL Warehouse, the warehouse_id will be populated here.                                                                                     |
| cluster_name              | string  | The name of the compute asset used to execute the task run                                                                                                                                                                    |
| job_cluster_key           | string  | When the task compute is a job_cluster the name of the job_cluster will be provided here                                                                                                                                      |
| job_cluster               | struct  | When the task compute is a job_cluster, the cluster_definition of the job_cluster used to execute the task                                                                                                                    |
| new_cluster               | struct  | **LEGACY** + SparkSubmit jobs -- new clusters are no longer used for tasks except for sparkSubmit jobs as they cannot use job_clusters. Job_clusters are used everywhere else                                                 |
| tags                      | map     | Job tags at the time of the run                                                                                                                                                                                               |
| task_detail               | struct  | The details of the task logic such as notebook_task, sql_task, spark_python_task, etc.                                                                                                                                        |
| task_dependencies         | array   | The list of tasks the task depends on to be successful in order to run                                                                                                                                                        |
| task_runtime              | struct  | The runtime of the task from launch to termination (including compute spin-up time)                                                                                                                                           |
| task_execution_runtime    | struct  | The execution time of the task (excluding compute spin-up time)                                                                                                                                                               |
| task_type                 | string  | Type of task to be executed -- this should mirror the "type" selected in the "type" drop down in the job definition. May be null for submitRun as this jobType                                                                |
| schedule                  | struct  | Schedule by which the job should execute and whether or not it is paused                                                                                                                                                      |
| libraries                 | array   | **LEGACY** -- Libraries defined in the job -- Nested within tasks as of API 2.1                                                                                                                                               |
| manual_override_params    | struct  | When task is executed manually and the default parameters were manually overridden the overridden parameters will be captured here                                                                                            |
| repair_details            | array   | Details of the repair run including any references to previous repairs                                                                                                                                                        |
| timeout_seconds           | string  | Job-level timeout seconds. Databricks supports timeout seconds at both the job level and the task level. Task level timeout_seconds can be found nested within tasks                                                          |
| retry_on_timeout          | boolean | **LEGACY** -- whether or not to retry if a job run times out -- Nested within tasks as of API 2.1                                                                                                                             |
| max_retries               | long    | **LEGACY** -- Max retries for legacy jobs -- Nested within tasks as of API 2.1                                                                                                                                                |
| min_retry_interval_millis | long    | **LEGACY** -- Minimal interval in milliseconds between the start of the failed run and the subsequent retry run. The default behavior is that unsuccessful runs are immediately retried. -- Nested within tasks as of API 2.1 |
| max_concurrent_runs       | long    | Job-level -- maximum concurrent executions of the job                                                                                                                                                                         |
| run_as_user_name          | string  | The user email of the principal configured to execute the job                                                                                                                                                                 |
| parent_run_id             | long    | The upstream run_id of the run that called current run using dbutils.notebook.run -- DO NOT confuse this with multitask_parent_run_id, these are different                                                                    |
| workflow_context          | string  | The workflow context (as a json string) provided when using Notebook Workflows (i.e. dbutils.notebook.run)                                                                                                                    |
| task_detail_legacy        | struct  | **LEGACY** -- The details of the task logic for legacy jobs such as notebook_task, spark_python_task, etc. These must be separated from the task level details as the structures have been altered in many cases              |
| submitRun_details         | struct  | When task_type == submitRun, full job and run definition provided in the submitRun API Call. Since no existing job definition is present for a submitRun -- all the details of the run submission are captured here           |
| created_by                | string  | Email account that created the job                                                                                                                                                                                            |
| last_edited_by            | string  | Email account that made the previous edit -- defaults to created by if no edits made                                                                                                                                          |
| request_detail            | struct  | All request details of the lifecycle and their results are captured here including submission, cancellation, completions, and execution start                                                                                 |
| time_detail               | struct  | All events in the run lifecycle timestamps are captured here in the event deeper timestamp analysis is required                                                                                                               |

#### JobRunCostPotentialFact
[**SAMPLE**](/assets/TableSamples/jobruncostpotentialfact.tab)

Databricks has moved to "multi-task jobs" and each run now refers to the run of a task not a job. Please reference 
[jobRuns table](#jobrun) for more detail

##### Unsupported Scenarios
* Costs for runs of that execute SQL/DBT/DLT tasks

**KEY** -- organization_id + run_id + startEpochMS

**Incremental Columns** -- startEpochMS

**Partition Columns** -- organization_id

**Write Mode** -- Merge

This fact table defines the job, the cluster, the cost, the potential, and utilization (if cluster logging is enabled) 
of a cluster associated with a specific Databricks Job Run.

**Dimensionality** Note that this fact table is not normalized by time but rather by job run and cluster state. 
Costs are not derived from job runs but from clusters thus the state\[s\] of the cluster are what's pertinent when
tying to cost. This is **extremely important** in the case of long running jobs, such as streaming. 

SCENARIO: <br>
Imagine a streaming job with 12 concurrent runs on an existing cluster that run for 20 days at the end of which the 
driver dies for some reason causing all runs fail and begin retrying but failing. When the 20 days end, the cost will 
be captured solely on that date and even more importantly, not only will all 20 days be captured at that date but the 
cost associated will be cluster runtime for 20 days * number of runs. Overwatch will automatically smooth the costs
across the concurrent runs but not the days running since this fact table is not based by on an equidistant time axis. 

* **Potential:** Total core_milliseconds for which the cluster COULD execute spark tasks. This derivation only includes
  the worker nodes in a state ready to receive spark tasks (i.e. Running). Nodes being added or running init scripts
  are not ready for spark jobs thus those core milliseconds are omitted from the total potential.
* **Cost:** Derived from the [instanceDetails](#instancedetails) table and DBU configured contract price (see
  [**Configuration**](({{%relref "GettingStarted/Configuration.md"%}})) for more details).
  The compute costs in [instanceDetails](#instancedetails) table are taken from the "Compute_Contract_Price" values associated with the
  instance type in instanceDetails.
* **Utilization:** Utilization is a function of core milliseconds used during spark task execution divided by the total
  amount of core milliseconds available given the cluster size and state. (i.e. spark_task_runtime_H / worker_potential_core_H)
* **Cluster State:** The state\[s\] of a cluster during a run. As the cluster scales and morphs to accommodate the run's 
  needs, the state changes. The number of state changes are recorded in this table as "run_cluster_states".
* **Run State:** Advanced Topic for data engineers and developers. This topic is discussed in considerable detail in the 
  [Advanced Topics]({{%relref "GettingStarted/AdvancedTopics.md"%}}) section.
  Given a cluster state, the run state is a state of all runs on a cluster at a given moment in time. This 
  is the measure used to calculate shared costs across concurrent runs. A run state cannot pass the boundaries of 
  a cluster state, a run that continues across cluster-state lines will result in a new run state.

| Column                  | Type        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|:------------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| organization_id         | string      | Canonical workspace id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| workspace_name          | string      | Customer defined name of the workspace or workspace_id (default)                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| job_id                  | long        | Canonical ID of job                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| job_name                | string      | Name of the runName if run is named, otherwise it will be job name                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| job_trigger_type        | string      | One of "cron" (automated scheduled), "manual", "repair"                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| terminal_state          | string      | State of the task run at the time of Overwatch pipeline execution                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| run_id                  | long        | The lowest level of the run_id (i.e. legacy jobs may not have a task_run_id, in this case, it will be the job_run_id).                                                                                                                                                                                                                                                                                                                                                                                                   |
| run_name                | string      | The name of the run if the run is named (i.e. in submitRun) otherwise this is set == taskKey                                                                                                                                                                                                                                                                                                                                                                                                                             |
| multitask_parent_run_id | long        | If the task belongs to a multi-task job the job_run_id will be populated here, otherwise it will be null                                                                                                                                                                                                                                                                                                                                                                                                                 |
| job_run_id              | long        | The run id of the job, not the task                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| task_run_id             | long        | The run id of the task except for legacy and                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| repair_id               | long        | If the task or job was repaired, the repair id will be present here and the details of the repair will be in repair_details                                                                                                                                                                                                                                                                                                                                                                                              |
| task_key                | string      | The name of the task is actually a key and must be unique within a job, this field specifies the task that was executed in this task_run_id                                                                                                                                                                                                                                                                                                                                                                              |
| task_type               | string      | Type of task to be executed -- this should mirror the "type" selected in the "type" drop down in the job definition. May be null for submitRun as this jobType                                                                                                                                                                                                                                                                                                                                                           |
| task_runtime            | struct      | The runtime of the task from launch to termination (including compute spin-up time)                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| task_execution_runtime  | struct      | The execution time of the task (excluding compute spin-up time)                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| cluster_type            | string      | Type of cluster used in the execution, one of "new", "job_cluster", "existing", "SQL Warehouse", null -- will be null for DLT pipelines and/or in situations where the type is not provided from Databricks                                                                                                                                                                                                                                                                                                              |
| cluster_id              | string      | The cluster ID of the compute used to execute the task run. If task executed on a SQL Warehouse, the warehouse_id will be populated here.                                                                                                                                                                                                                                                                                                                                                                                |
| cluster_name            | string      | The name of the compute asset used to execute the task run                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| cluster_tags            | map         | Tags present on the compute that executed the run                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| parent_run_id           | long        | The upstream run_id of the run that called current run using dbutils.notebook.run -- DO NOT confuse this with multitask_parent_run_id, these are different                                                                                                                                                                                                                                                                                                                                                               |
| running_days            | array<date> | Array (or list) of dates (not strings) across which the job run executed. This simplifies day-level cost attribution, among other metrics, when trying to smooth costs for long-running / streaming jobs                                                                                                                                                                                                                                                                                                                 |
| avg_cluster_share       | double      | Average share of the cluster the run had available assuming fair scheduling. This DOES NOT account for activity outside of jobs (i.e. interactive notebooks running alongside job runs), this measure only splits out the share among concurrent job runs. Measure is only calculated for interactive clusters, automated clusters assume 100% run allocation. For more granular utilization detail, enable cluster logging and utilize "job_run_cluster_util" column which derives utilization at the spark task level. |
| avg_overlapping_runs    | double      | Number of concurrent runs shared by the cluster on average throughout the run                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| max_overlapping_runs    | long        | Highest number of concurrent runs on the cluster during the run                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| run_cluster_states      | long        | Count of cluster states during the job run                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| driver_node_type_id     | string      | Driver Node type for the compute asset (not supported for Warehouses yet)                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| node_type_id            | string      | Worker Node type for the compute asset (not supported for Warehouses yet)                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| worker_potential_core_H | double      | cluster core hours capable of executing spark tasks, "potential"                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| dbu_rate                | double      | Effective DBU rate at time of job run used for calculations based on configured contract price in [instanceDetails](#instanceDetails) at the time of the Overwatch Pipeline Run                                                                                                                                                                                                                                                                                                                                          |
| driver_compute_cost     | double      | Compute costs associated with driver runtime                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| driver_dbu_cost         | double      | DBU costs associated with driver runtime                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| worker_compute_cost     | double      | Compute costs associated with worker runtime                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| worker_dbu_cost         | double      | DBU costs associated with cumulative runtime of all worker nodes                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| total_driver_cost       | double      | Driver costs including DBUs and compute                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| total_worker_cost       | double      | Worker costs including DBUs and compute                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| total_compute_cost      | double      | All compute costs for Driver and Workers                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| total_dbu_cost          | double      | All dbu costs for Driver and Workers                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| total_cost              | double      | Total cost from Compute and DBUs for all nodes (including Driver)                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| spark_task_runtimeMS    | long        | Spark core execution time in milliseconds (i.e. task was operating/locking on core)                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| spark_task_runtime_H    | double      | Spark core execution time in Hours (i.e. task was operating/locking on core)                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| job_run_cluster_util    | double      | Cluster utilization: spark task execution time / cluster potential. True measure by core of utilization. Only available when cluster logging is enabled.                                                                                                                                                                                                                                                                                                                                                                 |
| created_by              | string      | Email account that created the job                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| last_edited_by          | string      | Email account that made the previous edit -- defaults to created by if no edits made                                                                                                                                                                                                                                                                                                                                                                                                                                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

#### sqlQueryHistory
[**SAMPLE**](/assets/TableSamples/sqlqueryhistory.tab)

**KEY** -- organization_id + warehouse_id + query_id + query_start_time_ms

**Incremental Columns** -- query_start_time_ms

**Partition Columns** -- organization_id

**Write Mode** -- Merge

| Column                | Type   | Description                                                                                         |
|:----------------------|:-------|:----------------------------------------------------------------------------------------------------|
| organization_id       | string | Canonical workspace id                                                                              |
| workspace_name        | string | Customizable human-legible name of the workspace, should be globally unique within the organization |
| warehouse_id          | string | ID of the SQL warehouse.                                                                            |
| query_id              | string | ID of the query executed in the warehouse                                                           |
| query_end_time_ms     | long   | Query execution end time                                                                            |
| user_name             | string | User name who created the query                                                                     |
| user_id               | long   | Id of the user who created the query                                                                |
| executed_as_user_id   | long   | Id of the user who executed the query                                                               |
| executed_as_user_name | string | User name who executed the query                                                                    |
| duration              | long   | Duration of the query execution                                                                     |
| error_message         | string | Error message for failed queries                                                                    |
| execution_end_time_ms | long   | Query execution end time in ms                                                                      |
| query_start_time_ms   | long   | Query start time in ms                                                                              |
| query_text            | text   | Query text which is executed in the warehouse                                                       |
| rows_produced         | long   | Number of rows returned as query output                                                             |
| spark_ui_url          | string | URL of the Spark UI                                                                                 |
| statement_type        | string | Statement type of the query being executed, e.g - Select, Update etc                                |
| status                | string | Current status of the query being executed, e.g - FINISHED, RUNNING etc                             |
| compilation_time_ms   | long   | Query compilation time in ms                                                                        |
| execution_time_ms     | long   | Query execution time in ms                                                                          |
| network_sent_bytes    | long   | Size of data transferred over network in bytes                                                      |
| photon_total_time_ms  | long   | Total time in ms Photon engine executed                                                             |
| pruned_bytes          | long   | Size of data pruned in bytes                                                                        |
| pruned_files_count    | long   | Total number of files pruned                                                                        |
| read_bytes            | long   | Size of data red in bytes                                                                           |
| read_cache_bytes      | long   | Size of data cached during reading in bytes                                                         |
| read_files_count      | long   | Total number of files in read                                                                       |
| read_partitions_count | long   | Total number of partitions used while reading                                                       |
| read_remote_bytes     | long   |                                                                                                     |
| result_fetch_time_ms  | long   | Time taken in ms to fetch the results                                                               |
| result_from_cache     | long   | Flag to check whether result is fetched from cache                                                  |
| rows_produced_count   | long   | Total number of rows produced after fetching the data                                               |
| rows_read_count       | string | Total number of rows in the output after fetcing the data                                           |
| spill_to_disk_bytes   | long   | Data spilled to disk in bytes                                                                       |
| task_total_time_ms    | long   | Total time taken by task to complete in ms                                                          |
| total_time_ms         | long   | Total time taken by query in ms                                                                     |
| write_remote_bytes    | long   |                                                                                                     |

#### Notebook
[**SAMPLE**](/assets/TableSamples/notebook.tab)

**KEY** -- organization_id + notebook_id + request_id + action + unixTimeMS

**Incremental Columns** -- unixTimeMS

**Partition Columns** -- organization_id

**Write Mode** -- Append

| Column        | Type      | Description                                                                          |
|:--------------|:----------|:-------------------------------------------------------------------------------------|
| notebook_id   | string    | Canonical notebook id                                                                |
| notebook_name | string    | Name of notebook at time of action requested                                         |
| notebook_path | string    | Path of notebook at time of action requested                                         |
| cluster_id    | string    | Canonical workspace cluster id                                                       |
| action        | string    | action recorded                                                                      |
| timestamp     | timestamp | timestamp the action took place                                                      |
| old_name      | string    | When action is "renameNotebook" this holds notebook name before rename               |
| old_path      | string    | When action is "moveNotebook" this holds notebook path before move                   |
| new_name      | string    | When action is "renameNotebook" this holds notebook name after rename                |
| new_path      | string    | When action is "moveNotebook" this holds notebook path after move                    |
| parent_path   | string    | When action is "renameNotebook" notebook containing, workspace path is recorded here |
| user_email    | string    | Email of the user requesting the action                                              |
| request_id    | string    | Canonical request_id                                                                 |
| response      | struct    | HTTP response including errorMessage, result, and statusCode                         |

#### InstancePool

**KEY** -- organization_id + instance_pool_id + timestamp

**Incremental Columns** -- timestamp

**Partition Columns** -- organization_id

**Write Mode** -- Merge

| Column                                | Type   | Description                                              |
|:--------------------------------------|:-------|:---------------------------------------------------------|
| instance_pool_id                      | string | Canonical notebook id                                    |
| instance_pool_name                    | string | Name of notebook at time of action requested             |
| actionName                            | string | action recorded                                          |
| timestamp                             | long   | timestamp the action took place                          |
| node_type_id                          | string | Type of node in the pool                                 |
| idle_instance_autotermination_minutes | long   | Minutes after which a node shall be terminated if unused |
| min_idle_instances                    | long   | Minimum number of hot instances in the pool              |
| max_capacity                          | long   | Maximum number of nodes allowed in the pool              |
| preloaded_spark_versions              | string | Spark versions preloaded on nodes in the pool            |

#### Account Tables

{{% notice note%}}
**Not exposed in the consumer database**. These tables contain more sensitive information and by default are not
exposed in the consumer database but held back in the ETL database. This is done purposely to simplify security when/if
desired. If desired, this can be exposed in consumer database with a simple vew definition exposing the columns desired.
{{% /notice %}}

{{% notice note%}}
For deeper insights regarding audit, please reference [auditLogSchema](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html#request-parameters).
This is simplified through the use of the *ETL_DB.audit_log_bronze* and filter where serviceName == accounts for example.
Additionally, you may filter down to specific actions using "actionName". An example query is provided below:
{{% /notice %}}

```scala
spark.table("overwatch.audit_log_bronze")
  .filter('serviceName === "accounts" && 'actionName === "createGroup")
  .selectExpr("*", "requestParams.*").drop("requestParams")      
```

Slow changing dimension of user entity through time. Also used as reference map from user_email to user_id

| Column                | Type   | Description                                                                          |
|:----------------------|:-------|:-------------------------------------------------------------------------------------|
| organization_id       | string | Canonical workspace id                                                               |
| user_id               | string | Canonical user id for which the action was requested (within the workspace) (target) |
| user_email            | string | User's email for which the action was requested (target)                             |
| action                | string | Action requested to be performed                                                     |
| added_from_ip_address | string | Source IP of the request                                                             |
| added_by              | string | Authenticated user that made the request                                             |
| user_agent            | string | request origin such as browser, terraform, api, etc.                                 |

#### AccountMod
[**SAMPLE**](/assets/TableSamples/accountmodificationfact.tab)

**KEY** -- organization_id + acton + mod_unixTimeMS + request_id

**Incremental Columns** -- mod_unixTimeMS

**Partition Columns** -- organization_id

**Write Mode** -- Append

TODO

#### AccountLogin
[**SAMPLE**](/assets/TableSamples/accountloginfact.tab)

**KEY** -- organization_id + login_type + login_unixTimeMS + from_ip_address

**Incremental Columns** -- login_unixTimeMS

**Partition Columns** -- organization_id

**Write Mode** -- Append

{{% notice note%}}
**Not exposed in the consumer database**. This table contains more sensitive information and by default is not
exposed in the consumer database but held back in the etl datbase. This is done purposely to simplify security when/if
desired. If desired, this can be exposed in consumer database with a simple vew definition exposing the columns desired.
{{% /notice %}}

| Column                | Type   | Description                                           |
|:----------------------|:-------|:------------------------------------------------------|
| user_id               | string | Canonical user id (within the workspace)              |
| user_email            | string | User's email                                          |
| login_type            | string | Type of login such as web, ssh, token                 |
| ssh_username          | string | username used to login via SSH                        |
| groups_user_name      | string | ?? To research ??                                     |
| account_admin_userID  | string | ?? To research ??                                     |
| login_from_ip_address | struct | Details about the source login and target logged into |
| user_agent            | string | request origin such as browser, terraform, api, etc.  |

{{% notice note%}}
The following sections are related to Spark. Everything that can be seend/found in the SparkUI is visibel in the 
spark tables below. A reasonable understanding of the Spark hierarchy is necessary to 
make this section simpler. Please [**reference Spark Hierarchy For More Details**](({{%relref "GettingStarted/Modules.md"%}}/#sparkevents)) for more details.
{{% /notice %}}

#### SparkExecution
[**SAMPLE**](/assets/TableSamples/sparkexecution.tab)

**KEY** -- organization_id + spark_context_id + execution_id + date + unixTimeMS

**Incremental Columns** -- date + unixTimeMS

**Partition Columns** -- organization_id

**Write Mode** -- Merge

| Column                | Type   | Description                                           |
|:----------------------|:-------|:------------------------------------------------------|
| organization_id       | string | Canonical workspace id                                |
| spark_context_id      | string | Canonical context ID -- One Spark Context per Cluster |
| cluster_id            | string | Canonical workspace cluster id                        |
| execution_id          | long   | Spark Execution ID                                    |
| description           | string | Description provided by spark                         |
| details               | string | Execution StackTrace                                  |
| sql_execution_runtime | struct | Complete runtime detail breakdown                     |

#### SparkExecutor
[**SAMPLE**](/assets/TableSamples/sparkexecutor.tab)

**KEY** -- organization_id + spark_context_id + executor_id + date + unixTimeMS

**Incremental Columns** -- date + unixTimeMS

**Partition Columns** -- organization_id

**Write Mode** -- Merge

| Column             | Type   | Description                                           |
|:-------------------|:-------|:------------------------------------------------------|
| organization_id    | string | Canonical workspace id                                |
| spark_context_id   | string | Canonical context ID -- One Spark Context per Cluster |
| cluster_id         | string | Canonical workspace cluster id                        |
| executor_id        | int    | Executor ID                                           |
| executor_info      | string | Executor Detail                                       |
| removed_reason     | string | Reason executor was removed                           |
| executor_alivetime | struct | Complete lifetime detail breakdown                    |

#### SparkJob
[**SAMPLE**](/assets/TableSamples/sparkjob.tab)

**KEY** -- organization_id + spark_context_id + job_id + unixTimeMS

**Incremental Columns** -- date + unixTimeMS

**Partition Columns** -- organization_id + date

**Z-Order Columns** -- cluster_id

**Write Mode** -- Merge

| Column           | Type          | Description                                                                                                                         |
|:-----------------|:--------------|:------------------------------------------------------------------------------------------------------------------------------------|
| organization_id  | string        | Canonical workspace id                                                                                                              |
| spark_context_id | string        | Canonical context ID -- One Spark Context per Cluster                                                                               |
| cluster_id       | string        | Canonical workspace cluster id                                                                                                      |
| job_id           | string        | Spark Job ID                                                                                                                        |
| job_group_id     | string        | Spark Job Group ID -- NOTE very powerful for many reasons. See [SparkEvents]({{%relref "GettingStarted/Modules.md"%}}/#sparkevents) |
| execution_id     | string        | Spark Execution ID                                                                                                                  |
| stage_ids        | array\[long\] | Array of all Spark Stage IDs nested within this Spark Job                                                                           |
| notebook_id      | string        | Canonical Databricks Workspace Notebook ID                                                                                          |
| notebook_path    | string        | Databricks Notebook Path                                                                                                            |
| user_email       | string        | email of user that owned the request, for Databricks jobs this will be the job owner                                                |
| db_job_id        | string        | Databricks Job Id executing the Spark Job                                                                                           |
| db_id_in_job     | string        | "id_in_job" such as "Run 10" without "Run " prefix. This is a critical join column when working looking up Databricks Jobs metadata |
| job_runtime      | string        | Complete job runtime detail breakdown                                                                                               |
| job_result       | struct        | Job Result and Exception if present                                                                                                 |

#### SparkStage
[**SAMPLE**](/assets/TableSamples/sparkstage.tab)

**KEY** -- organization_id + spark_context_id + stage_id + stage_attempt_id + unixTimeMS

**Incremental Columns** -- date + unixTimeMS

**Partition Columns** -- organization_id + date

**Z-Order Columns** -- cluster_id

**Write Mode** -- Merge

| Column           | Type   | Description                                           |
|:-----------------|:-------|:------------------------------------------------------|
| organization_id  | string | Canonical workspace id                                |
| spark_context_id | string | Canonical context ID -- One Spark Context per Cluster |
| cluster_id       | string | Canonical workspace cluster id                        |
| stage_id         | string | Spark Stage ID                                        |
| stage_attempt_id | string | Spark Stage Attempt ID                                |
| stage_runtime    | string | Complete stage runtime detail                         |
| stage_info       | string | Lineage of all accumulables for the Spark Stage       |

#### SparkTask
[**SAMPLE**](/assets/TableSamples/sparktask.tab)

**KEY** -- organization_id + spark_context_id + task_id + task_attempt_id + stage_id + stage_attempt_id + host + unixTimeMS

**Incremental Columns** -- date + unixTimeMS

**Partition Columns** -- organization_id + date

**Z-Order Columns** -- cluster_id

**Write Mode** -- Merge

{{% notice warning%}}
**USE THE PARTITION COLUMN** (date) and Indexed Column (cluster_id) in all joins and filters where possible. 
This table can get extremely large, select samples or smaller date ranges and reduce joins and columns selected 
to improve performance. 
{{% /notice %}}

| Column           | Type   | Description                                                                                                                      |
|:-----------------|:-------|:---------------------------------------------------------------------------------------------------------------------------------|
| organization_id  | string | Canonical workspace id                                                                                                           |
| workspace_name   | string | Customizable human-legible name of the workspace, should be globally unique within the organization                              |
| spark_context_id | string | Canonical context ID -- One Spark Context per Cluster                                                                            |
| cluster_id       | string | Canonical workspace cluster id                                                                                                   |
| task_id          | string | Spark Task ID                                                                                                                    |
| task_attempt_id  | string | Spark Task Attempt ID                                                                                                            |
| stage_id         | string | Spark Stage ID                                                                                                                   |
| stage_attempt_id | string | Spark Stage Attempt ID                                                                                                           |
| executor_id      | string | Spark Executor ID                                                                                                                |
| host             | string | Internal IP address of node                                                                                                      |
| task_runtime     | string | Complete task runtime detail                                                                                                     |
| task_metrics     | string | Lowest level compute metrics provided by spark such as spill bytes, read/write bytes, shuffle info, GC time, Serialization, etc. |
| task_info        | string | Lineage of all accumulables for the Spark Task                                                                                   |
| task_type        | string | Spark task Type (i.e. ResultTask, ShuffleMapTask, etc)                                                                           |
| task_end_reason  | string | Task end status, state, and details plus stake trace when error                                                                  |

#### SparkStream_preview
**KEY** -- organization_id + spark_context_id + cluster_id + stream_id + stream_run_id + stream_batch_id + stream_timestamp

**Incremental Columns** -- date + stream_timestamp

**Partition Columns** -- organization_id + date

**Z-Order Columns** -- cluster_id

**Write Mode** -- Merge

Remains in preview through version 0.6.0 as more feedback is requested from users and use-cases before this table 
structure solidifes.

| Column            | Type           | Description                                                                                                    |
|:------------------|:---------------|:---------------------------------------------------------------------------------------------------------------|
| spark_context_id  | string         | Canonical context ID -- One Spark Context per Cluster                                                          |
| cluster_id        | string         | Canonical workspace cluster id                                                                                 |
| stream_id         | string         | GUID ID of the spark stream                                                                                    |
| stream_name       | string         | Name of stream if named                                                                                        |
| stream_run_id     | string         | GUID ID of the spark stream run                                                                                |
| stream_batch_id   | long           | GUID ID of the spark stream run batch                                                                          |
| stream_timestamp  | long           | Unix time (millis) the stream reported its batch complete metrics                                              |
| streamSegment     | string         | Type of event from the event listener such as 'Progressed'                                                     |
| streaming_metrics | dynamic struct | All metrics available for the stream batch run                                                                 |
| execution_ids     | array<long>    | Array of execution_ids in the spark_context. Can explode and tie back to sparkExecution and other spark tables |

#### Common Meta Fields
| Column          | Type   | Description                                                       |
|:----------------|:-------|:------------------------------------------------------------------|
| organization_id | string | Workspace / Organization ID on which the cluster was instantiated |
| cluster_id      | string | Canonical workspace cluster id                                    |
| unixTimeMS      | long   | unix time epoch as a long in milliseconds                         |
| timestamp       | string | unixTimeMS as a timestamp type in milliseconds                    |
| date            | string | unixTimeMS as a date type                                         |
| created_by      | string |                                                                   |
| last_edited_by  | string | last user to edit the state of the entity                         |
| last_edited_ts  | string | timestamp at which the entitiy's sated was last edited            |
| deleted_by      | string | user that deleted the entity                                      |
| deleted_ts      | string | timestamp at which the entity was deleted                         |
| event_log_start | string | Spark Event Log BEGIN file name / path                            |
| event_log_end   | string | Spark Event Log END file name / path                              |
| Pipeline_SnapTS | string | Snapshot timestmap of Overwatch run that added the record         |
| Overwatch_RunID | string | Overwatch canonical ID that resulted in the record load           |

## ETL Tables
The following are the list of potential tables, the module with which it's created and the layer in which it lives.
This list consists of only the ETL tables created to facilitate and deliver the [consumption layer](#consumption-layer-tables)
<br><br>
The gold and consumption layers are the only layers that maintain column name uniformity and naming convention across
all tables. Users should always reference Consumption and Gold layers unless the data necessary has not been curated.

### Bronze
| Table                       | Scope         | Layer          | Description                                                                                                                                                                                                                                                                                                                     |
|:----------------------------|:--------------|:---------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| audit_log_bronze            | audit         | bronze         | Raw audit log data full schema                                                                                                                                                                                                                                                                                                  |
| audit_log_raw_events        | audit         | bronze (azure) | Intermediate staging table responsible for coordinating intermediate events from azure Event Hub                                                                                                                                                                                                                                |
| cluster_events_bronze       | clusterEvents | bronze         | Raw landing of dataframe derived from JSON response from cluster events api call. Note: cluster events expire after 30 days of last termination. ([reference](https://docs.databricks.com/dev-tools/api/latest/clusters.html#events))                                                                                           |
| clusters_snapshot_bronze    | clusters      | bronze         | API snapshot of existing clusters defined in Databricks workspace at the time of the Overwatch run. Snapshot is taken on each run                                                                                                                                                                                               |
| jobs_snapshot_bronze        | jobs          | bronze         | API snapshot of existing jobs defined in Databricks workspace at the time of the Overwatch run. Snapshot is taken on each run                                                                                                                                                                                                   |
| pools_snapshot_bronze       | pools         | bronze         | API snapshot of existing pools defined in Databricks workspace at the time of the Overwatch run. Snapshot is taken on each run                                                                                                                                                                                                  |
| spark_events_bronze         | sparkEvents   | bronze         | Raw landing of the master sparkEvents schema and data for all cluster logs. Cluster log locations are defined by cluster specs and all locations will be scanned for new files not yet captured by Overwatch. Overwatch uses an implicit schema generation here, as such, **a lack of real-world can cause unforeseen issues**. |
| spark_events_processedfiles | sparkEvents   | bronze         | Table that keeps track of all previously processed cluster log files (spark event logs) to minimize future file scanning and improve performance. This table can be used to reprocess and/or find specific eventLog files.                                                                                                      |
| pipeline_report             | NA            | tracking       | Tracking table used to identify state and status of each Overwatch Pipeline run. This table is also used to control the start and end points of each run. Altering the timestamps and status of this table will change the ETL start/end points.                                                                                |

### Silver
| Table                        | Scope         | Layer  | Description                                                                                                                                                                                                                                                     |
|:-----------------------------|:--------------|:-------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| account_login_silver         | accounts      | silver | Login events                                                                                                                                                                                                                                                    |
| account_mods_silver          | accounts      | silver | Account modification events                                                                                                                                                                                                                                     |
| cluster_spec_silver          | clusters      | silver | Slow changing dimension used to track all clusters through time including edits but **excluding state change**.                                                                                                                                                 |
| cluster_state_detail_silver  | clusterEvents | silver | State detail for each cluster event enriched with cost information                                                                                                                                                                                              |
| job_status_silver            | jobs          | silver | Slow changing dimension used to track all jobs specifications through time                                                                                                                                                                                      |
| jobrun_silver                | jobs          | silver | Historical run of every job since Overwatch began capturing the audit_log_data                                                                                                                                                                                  |
| notebook_silver              | notebooks     | silver | Slow changing dimension used to track all notebook changes as it morphs through time along with which user instigated the change. This does not include specific change details of the commands within a notebook just metadata changes regarding the notebook. |
| pools_silver                 | pools         | silver | Slow changing dimension used to track all changes to instance pools                                                                                                                                                                                             |
| spark_executions_silver      | sparkEvents   | silver | All spark event data relevant to spark executions                                                                                                                                                                                                               |
| spark_executors_silver       | sparkEvents   | silver | All spark event data relevant to spark executors                                                                                                                                                                                                                |
| spark_jobs_silver            | sparkEvents   | silver | All spark event data relevant to spark jobs                                                                                                                                                                                                                     |
| spark_stages_silver          | sparkEvents   | silver | All spark event data relevant to spark stages                                                                                                                                                                                                                   |
| spark_tasks_silver           | sparkEvents   | silver | All spark event data relevant to spark tasks                                                                                                                                                                                                                    |
| sql_query_history_silver     | sqlHistory    | silver | History of all the sql queries executed through SQL warehouses                                                                                                                                                                                                  |

### Gold
| Table                   | Scope         | Layer | Description                                                                                                                                                                                                                                                                                                                                                                        |
|:------------------------|:--------------|:------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| account_login_gold      | accounts      | gold  | Login events                                                                                                                                                                                                                                                                                                                                                                       |
| account_mods_gold       | accounts      | gold  | Account modification events                                                                                                                                                                                                                                                                                                                                                        |
| cluster_gold            | clusters      | gold  | Slow-changing dimension with all cluster creates and edits through time. These events **DO NOT INCLUDE automated cluster resize events or cluster state changes**. Automated cluster resize and cluster state changes will be in clusterstatefact_gold. If user changes min/max nodes or node count (non-autoscaling) the event will be registered here AND clusterstatefact_gold. |
| clusterStateFact_gold   | clusterEvents | gold  | All cluster event changes along with the time spent in each state and the core hours in each state. This table should be used to find cluster anomalies and/or calculate compute/DBU costs of some given scope.                                                                                                                                                                    |
| job_gold                | jobs          | gold  | Slow-changing dimension of all changes to a job definition through time                                                                                                                                                                                                                                                                                                            |
| jobrun_gold             | jobs          | gold  | Dimensional data for each job run in the databricks workspace                                                                                                                                                                                                                                                                                                                      |
| notebook_gold           | notebooks     | gold  | Slow changing dimension used to track all notebook changes as it morphs through time along with which user instigated the change. This does not include specific change details of the commands within a notebook just metadata changes regarding the notebook.                                                                                                                    |
| instancepool_gold       | pools         | gold  | Slow changing dimension used to track all changes to instance pools                                                                                                                                                                                                                                                                                                                |
| sparkexecution_gold     | sparkEvents   | gold  | All spark event data relevant to spark executions                                                                                                                                                                                                                                                                                                                                  |
| sparkexecutor_gold      | sparkEvents   | gold  | All spark event data relevant to spark executors                                                                                                                                                                                                                                                                                                                                   |
| sparkjob_gold           | sparkEvents   | gold  | All spark event data relevant to spark jobs                                                                                                                                                                                                                                                                                                                                        |
| sparkstage_gold         | sparkEvents   | gold  | All spark event data relevant to spark stages                                                                                                                                                                                                                                                                                                                                      |
| sparktask_gold          | sparkEvents   | gold  | All spark event data relevant to spark tasks                                                                                                                                                                                                                                                                                                                                       |
| sparkstream_gold        | sparkEvents   | gold  | All spark event data relevant to spark streams                                                                                                                                                                                                                                                                                                                                     |
| sql_query_history_gold  | sqlHistory    | gold  | History of all the sql queries executed through SQL warehouses                                                                                                                                                                                                                                                                                                                     |
