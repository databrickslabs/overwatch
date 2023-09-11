---
title: "Dashboards"
date: 2022-12-13T13:49:40-05:00
---

We have created a set of dashboards containing some essential, pre-defined metrics, to help you get started on your Overwatch journey.
These are meant to be a learning resource for you to understand the data model, as well as a practical resource to help you get value out of Overwatch right away.
As a first iteration, these are notebook-based dashboards, in the future we'll have these available in DBSQL as well.

## Available Dashboards

### Workspace
Start here, this is your initial overall view of the state of the workspaces you monitor with Overwatch

Metrics available in this dashboard
| ----------- | -----------|
| Daily cluster spend chart 					| Compute Time of scheduled jobs on each workspace |
| Cluster spend on each workspace 			| Node type count by azure workspace |
| DBU Cost vs Compute Cost 					| Node type count by AWS workspace |
| Cluster spend by type on each workspace 	| Node type cost by azure workspace |
| Cluster count by type on each workspace 	| Node type cost by AWS workspace |
| Count of scheduled jobs on each workspace 	| Workspace Tags count by workspace |

### Clusters
This dashboard will deep dive into cluster-specific metrics

Metrics available in this dashboard
| ----------- | -----------|
| DBU Spend by cluster category | Total DBU Incurred by top spending clusters per category |
| DBU spend by the most expensive cluster per day per workspace | Percentage of Autoscaling clusters per category |
| Top spending clusters per day Scale up time of clusters (with & without pools) by cluster category |
| DBU Spend by the top 3 expensive Interactive clusters (without auto-termination) per day | Cluster Failure States and count of failures |
| Cluster count in each category | Cost of cluster failures per Failure States per workspace |
| Cluster node type breakdown | Cluster Failure States and failure count distribution |
| Cluster node type breakdown by potential | Interactive cluster restarts per day per cluster |
| Cluster node potential breakdown by cluster category |

### Job
This dashboard will deep dive into job/workload specific metrics

Metrics available in this dashboard
| ----------- | -----------|
| Daily Cost on Jobs | Daily Job status distribution |
| Job Count by workspace | Number of job Runs (Succeeded vs Failed) |
| Jobs running in Interactive Clusters (Top 20 workspaces) | Compute Time of Run Failures By Workspace |

### Notebook
In this dashboard we will cover metrics that could help in the tuning of workloads by analyzing the code run in Notebooks. 

Metrics available in this dashboard
| ----------- | -----------|
| Data Throughput Per Notebook Path | Largest shuffle explosions
| Longest Running Notebooks | Total Spills per notebook run |
| Data Throughput Per Notebook Path | Largest shuffle explosions |
| Top notebooks returning a lot of data to the UI | Processing Speed (MB/sec)  |
| Spark Actions (count) | Longest Running Failed Spark Jobs |
| Notebooks with the largest records | Serialization/deserialization time (ExecutorDeserializeTime + ResultSerializationTime) |
| Task count by task type | Notebook compute hours |
| Large Tasks Count (> 400MB) | Most popular (distinct users) notebooks per path depth |
| Jobs Executing on Notebooks (count) | || Longest Running Notebooks | Total Spills per notebook run
| Top notebooks returning a lot of data to the UI | Processing Speed (MB/sec)
| Spark Actions (count) | Longest Running Failed Spark Jobs
| Notebooks with the largest records | Serialization/deserialization time (ExecutorDeserializeTime + ResultSerializationTime)
| Task count by task type | Notebook compute hours
| Large Tasks Count (> 400MB) | Most popular (distinct users) notebooks per path depth
| Jobs Executing on Notebooks (count)

### DBSQL
A generic view at the performance of your DBSQL-specific queries

Metrics available in this dashboard
| ----------- | -----------|
| Global query duration and Global Query Core Hours | Core hours by users (Top 20) |
| Query count through time | Distinct user count my warehouse |
| Query count by warehouse | Core hours by date |
| Core hours by warehouse | Core hours by Is Serverless |

## Dashboard files
Please download the dbc file and import it into your workspace, read through the readme, and you should be 
able to get them running right away. 

- Version 1 - [DBC](/assets/Dashboards/Dashboards_v1.0.dbc) Released September 11, 2023
