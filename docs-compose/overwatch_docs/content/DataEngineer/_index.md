---
title: "Data Engineering"
weight: 3
---

## Overwatch ERD
![OverwatchERD](/images/_index/Overwatch_Gold.png)
For full fidelity image, right click and save the image.

## Overwatch Data Promotion Process
Overwatch data is promoted from bronze - silver - gold - presentation to ensure data consistency and quality as the 
data is enriched between the stages. The presentation layer is composed of views that reference the latest schema
version of the gold layer. This disconnects the consumption layer from the underlying data structure so that developers
can transparently add and alter columns without user disruption. All tables in each layer (except consumption) are 
suffixed in the ETL database with *\_layer*. 

{{% notice note %}}
Note, the ETL database and consumption database are usually different,
determined by the [**configuration**]({{%relref "GettingStarted/Configuration.md"%}}).
{{% /notice %}}

## Common Terms & Concepts
### Snapshot
A snapshot is a point in time image of a context. For example, Cluster and Job definitions come and go in a 
Databricks workspace but at the time of an Overwatch run, there is a state and the snapshots capture this state.

Several *snapshot* tables can be found in the bronze layer such as *clusters_snapshot_bronze*. As a specific
example, this snapshot is an entire capture of all existing clusters and definitions at the time of the Overwatch run.
This is an important reference point, especially in the early days of running Overwatch, as depending on when the 
process and logging was begun, many of the existing clusters were created (and last edited) before the audit logs 
could capture it therefore, the existence and metadata of a cluster would be missing from Overwatch if not for the 
snapshot. The snapshots also serve as a good last resort lookup for any missing data through time, espeically as 
it pertains to assets that exist for a longer period of time.

Snapshots should not be the first choice when looking for data regarding slow changing assets (such as clusters) as 
the resolution is expectedly low, it's much better to hit the slow changing dimensions in gold, *cluster*, or even 
in silver such as *cluster_spec_silver*. If the raw data you require has not been enriched from raw state then you 
likely have no other choice than to go directly to the audit logs, *audit_log_bronze*, and filter down to the 
relevant *service_name* and *action*.

### Job
{{% notice note %}}
Be weary of the term **JOB** when working with Overwatch (and Spark on Databricks in general). A Databricks Job is
a very different entity than a Spark Job. The documentation attempts to be very clear, please file a ticket if you 
find a section in which the difference is not made clear.
{{% /notice %}}

### [Spark Context Composition & Hierarchy]({{%relref "GettingStarted/Modules.md"%}})