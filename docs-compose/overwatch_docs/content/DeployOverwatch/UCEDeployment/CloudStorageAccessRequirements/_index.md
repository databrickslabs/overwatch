---
title: "UC Storage Requirements"
date: 2023-04-18T11:28:39-05:00
weight: 1
---

As a prerequisite for Overwatch UC Enablement it is required that Overwatch Job Cluster have 
Read/Write access to UC external location(s3 bucket, Azure ADLS container, GC Bucket ) and Read (at least) 
to the cluster log storage locations. Please find the specific configuration based on the cloud provider in the below section.

This access is required because the Overwatch pipelines utilize low-level file access from executors to optimize 
interaction with log files produced from the clusters. Going directly to the files to scan modification dates and 
validate sizes / etc are much more efficient (12-20x) then doing this sequentially through the external location APIS.
Databricks is in the process of making "Volumes" GA and Overwatch will continue to monitor capabilities and remove 
these requirements as soon as its feasible.

* [Azure]({{%relref "/DeployOverwatch/UCEDeployment/CloudStorageAccessRequirements/Azure.md"%}})
* [AWS]({{%relref "/DeployOverwatch/UCEDeployment/CloudStorageAccessRequirements/AWS.md"%}}) 
* [GCP]({{%relref "/DeployOverwatch/UCEDeployment/CloudStorageAccessRequirements/GCP.md"%}})
