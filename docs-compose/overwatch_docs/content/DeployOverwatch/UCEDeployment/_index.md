---
title: "Deploying To Unity Catalog"
date: 2023-04-18T11:28:39-05:00
weight: 5
---

{{% notice note %}}
At this time Overwatch only supports Unity Catalog External Tables. Overwatch was designed to operate with external 
tables and this strategy has been mirrored to the UC deployments as well. We're investigating options for enabling 
Overwatch on UC Managed Tables.
{{% /notice %}}

Deploying Overwatch to a Unity Catalog is nearly exactly the same as deploying to a Hive Metastore with a few 
additional pre-requisites to configure auth for UC underlying storage, External Locations, Catalogs, and Schemas. 

This guide assumes the reader has an understanding of Unity Catalog, Storage Credentials, and External Locations. 
For more details on how these components work together to provide fine-grained ACLs to data and storage please see 
the [Databricks Docs for Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

## UC Deployment Specific Differences
As mentioned before the deployment process is nearly identical to deploying on hive metastore so all the docs in the 
[Deployment Guide]({{%relref "DeployOverwatch"%}}) are very relevant and should guide your deployment with the 
specific deviations referenced below and detailed out in this section of the docs.

* [Storage Requirements]({{%relref "DeployOverwatch/UCEDeployment/CloudStorageAccessRequirements/"%}})
* [UC Pre-Requisites]({{%relref "DeployOverwatch/UCEDeployment/UCEPreReqs"%}})
* [Configuration Details]({{%relref "DeployOverwatch/UCEDeployment/UCEConfiguration"%}})

## Migrating to UC from Hive Metastore
If you are currently on 0720+ in Hive Metastore and would like to migrate your deployment to UC, the process and 
script for doing that can be found in the [Migrating To UC]({{%relref "DeployOverwatch/UCEDeployment/MigratingToUC"%}})
section.