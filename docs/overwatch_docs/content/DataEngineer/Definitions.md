---
title: "Definitions"
date: 2021-01-11T12:21:19-05:00
draft: true
weight: 1
---

* [Consumption Layer](#consumption-layer-tables-views)
    * [Column Descriptions](#column-descriptions)
* [ETL Tables](#etl-tables)
    * [Bronze](#bronze)
    * [Silver](#silver)
    * [Gold](#gold)

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

![OverwatchERD](/images/DataEngineer/_index/Overwatch_Gold.png)

### Column Descriptions
Complete column descriptions are only provided for the consumption layer. The entity names are linked below.

* [cluster](#cluster)
* [clusterStateFact](#clusterstatefact)
* [job](#job)
* [jobrun](#jobrun)
* [notebook](#notebook)
* [user](#user)
* [userLoginFact](#userloginfact)
* [sparkExecution](#sparkexecution)
* [sparkExecutor](#sparkexecutor)
* [sparkJob](#sparkjob)
* [sparkStage](#sparkstage)
* [sparkTask](#sparktask)
* [Common Meta Fields](#common-meta-fields)
  * There are several fields that are present in all tables. Instead of cluttering each table with them, this section
  was created as a reference to each of these.

{{% notice info %}}
Most tables below provide a data **SAMPLE** that you can download and review in Excel or your favorite CSV viewer.
To do this, just right click the SAMPLE link and click saveTargetAs or saveLinkAs and save the file.
{{% /notice %}}

#### Cluster
[**SAMPLE**](/assets/TableSamples/cluster.csv)

Column | Type | Description
:---------------------------|:--------------|:--------------------------------------------------
cluster_id                  |string           |Canonical Databricks cluster ID (more info in [Common Meta Fields]())
action                      |string           |Either **create** OR **edit** -- depicts the type of action for the cluster 
unixTimeMS                  |long             |Epoch time in milliseconds the action occurred on the cluster
cluster_name                |string           |user-defined name of the cluster
driver_node_type            |string           |Canonical name of the driver node type.
node_type                   |string           |Canonical name of the worker node type.
num_workers                 |int              |The number of workers defined WHEN autoscaling is disabled
autoscale                   |struct           |The min/max workers defined WHEN autoscaling is enabled
auto_termination_minutes    |int              |The number of minutes before the cluster auto-terminates due to inactivity
enable_elastic_disk         |boolean          |Whether autoscaling disk was enabled or not
cluster_log_conf            |string           |Logging directory if configured
init_script                 |Array\[Struct\]  |Array of init scripts
custom_tags                 |string           |User-Defined tags AND also includes Databricks JobID and Databricks RunName when the cluster is created by a Databricks Job as an automated cluster. Other Databricks services that create clusters also store unique information here such as SqlEndpointID when a cluster is created by "SqlAnalytics" 
cluster_source              |string           |Shows the source of the action **(TODO -- checking on why null scenario with BUI)
spark_env_vars              |string           |Spark environment variables defined on the cluster
spark_conf                  |string           |custom spark configuration on the cluster that deviate from default
acl_path_prefix             |string           |Automated jobs pass acl to clusters via a path format, the path is defined here
instance_pool_id            |string           |Canononical pool id from which the cluster receives its nodes
spark_version               |string           |DBR version - scala version
idempotency_token           |string           |Idempotent jobs token if used
organization_id             |string           |Workspace / Organization ID on which the cluster was instantiated

#### ClusterStateFact

#### Job

#### JobRun

#### Notebook

#### User

#### UserLoginFact

#### SparkExecution

#### SparkExecutor

#### SparkJob

#### SparkStage

#### SparkTask

#### Common Meta Fields
Column | Type | Description
:---------------------------|:--------------|:--------------------------------------------------
cluster_id                  |string         | HOLD
timestamp                   |string         |unixTimeMS as a timestamp type in milliseconds
date                        |string         |unixTimeMS as a date type

## ETL Tables
The following are the list of potential tables, the module with which it's created and the layer in which it lives.
This list consists of only the ETL tables created to facilitate and deliver the [consumption layer](#consumption-layer-tables)
<br><br>
The gold and consumption layers are the only layers that maintain column name uniformity and naming convention across
all tables. Users should always reference Consumption and Gold layers unless the data necssary has not been curated.

### Bronze
Table | Module | Layer | Description
:---------------------------|:--------------|:--------------|:--------------------------------------------------
audit_log_bronze            |audit          |bronze         |Raw audit log data full schema
audit_log_raw_events        |audit          |bronze (azure) |Intermediate staging table responsible for coordinating intermediate events from azure Event Hub
cluster_events_bronze       |clusterEvents  |bronze         |Raw landing of dataframe derived from JSON response from cluster events api call. Note: cluster events expire after 30 days of last termination. ([reference](https://docs.databricks.com/dev-tools/api/latest/clusters.html#events))
clusters_snapshot_bronze    |clusters       |bronze         |API snapshot of existing clusters defined in Databricks workspace at the time of the Overwatch run. Snapshot is taken on each run
jobs_snapshot_bronze        |jobs           |bronze         |API snapshot of existing jobs defined in Databricks workspace at the time of the Overwatch run. Snapshot is taken on each run
pools_snapshot_bronze       |pools          |bronze         |API snapshot of existing pools defined in Databricks workspace at the time of the Overwatch run. Snapshot is taken on each run
spark_events_bronze         |sparkEvents    |bronze         |Raw landing of the master sparkEvents schema and data for all cluster logs. Cluster log locations are defined by cluster specs and all locations will be scanned for new files not yet captured by Overwatch. Overwatch uses an implicit schema generation here, as such, **a lack of real-world can cause unforeseen issues**. See [Spark Events]({{%relref "Developer_Docs/Spark Events.md"%}}) in Developer_Docs for more information.
spark_events_processedfiles |sparkEvents    |bronze         |Table that keeps track of all previously processed cluster log files (spark event logs) to minimize future file scanning and improve performance. This table can be used to reprocess and/or find specific eventLog files.
pipeline_report             |NA             |tracking       |Tracking table used to identify state and status of each Overwatch Pipeline run. This table is also used to control the start and end points of each run. Altering the timestamps and status of this table will change the ETL start/end points.

### Silver
Table | Module | Layer | Description
:---------------------------|:--------------|:--------------|:--------------------------------------------------
cluster_spec_silver         |clusters       |silver         |Slow changing dimension used to track all clusters through time including edits but **excluding state change**.
cluster_status_silver       |clusters       |silver         |**Deprecated** Originally used to track cluster state and scale through time but is no longer used and is incomplete.
job_status_silver           |jobs           |silver         |Slow changing dimension used to track all jobs specifications through time
jobrun_silver               |jobs           |silver         |Historical run of every job since Overwatch began capturing the audit_log_data
notebook_silver             |notebooks      |silver         |Slow changing dimension used to track all notebook changes as it morphs through time along with which user instigated the change. This does not include specific change details of the commands within a notebook just metadata changes regarding the notebook. 
spark_executions_silver     |sparkEvents    |silver         |All spark event data relevant to spark executions
spark_executors_silver      |sparkEvents    |silver         |All spark event data relevant to spark executors
spark_jobs_silver           |sparkEvents    |silver         |All spark event data relevant to spark jobs
spark_stages_silver         |sparkEvents    |silver         |All spark event data relevant to spark stages
spark_tasks_silver          |sparkEvents    |silver         |All spark event data relevant to spark tasks
user_account_silver         |accounts       |silver         |Slow changing dimension of user accounts through time
user_login_silver           |accounts       |silver         |User login metadata through time

### Gold
Table | Module | Layer | Description
:---------------------------|:--------------|:--------------|:--------------------------------------------------
cluster_gold                |clusters       |gold           |Slow-changing dimension with all cluster creates and edits through time. These events **DO NOT INCLUDE automated cluster resize events or cluster state changes**. Automated cluster resize and cluster state changes will be in clusterstatefact_gold. If user changes min/max nodes or node count (non-autoscaling) the event will be registered here AND clusterstatefact_gold.  
clusterStateFact_gold       |clusterEvents  |gold           |All cluster event changes along with the time spent in each state and the core hours in each state. This table should be used to find cluster anomalies and/or calculate compute/DBU costs of some given scope. 
job_gold                    |jobs           |gold           |Slow-changing dimension of all changes to a job definition through time
jobrun_gold                 |jobs           |gold           |Dimensional data for each job run in the databricks workspace
notebook_gold               |notebooks      |gold           |Slow changing dimension used to track all notebook changes as it morphs through time along with which user instigated the change. This does not include specific change details of the commands within a notebook just metadata changes regarding the notebook.
sparkexecution_gold         |sparkEvents    |gold           |All spark event data relevant to spark executions
sparkexecutor_gold          |sparkEvents    |gold           |All spark event data relevant to spark executors
sparkjob_gold               |sparkEvents    |gold           |All spark event data relevant to spark jobs
sparkstage_gold             |sparkEvents    |gold           |All spark event data relevant to spark stages
sparktask_gold              |sparkEvents    |gold           |All spark event data relevant to spark tasks
