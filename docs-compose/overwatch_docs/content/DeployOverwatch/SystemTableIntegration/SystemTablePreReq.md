---
title: "System Table Pre-Requisites"
date: 2024-01-23T16:27:22+05:30
---

This section will walk you through the steps necessary as a prerequisite for System Table Integration with Overwatch.

* **Workspace** should be **UC enabled** and **System Table enabled**. Enabling system tables is straightforward, 
you do need to have at least one unity-catalog enabled workspace. System tables must be enabled by an account admin. 
You can enable system tables using the Unity Catalog REST API.  Once enabled, the tables will appear in a catalog 
called system, which is included in every Unity Catalog metastore. In the system catalog you’ll see schemas such as 
access and billing that contain the system tables. Follow the documents to enable 
[System Tables](https://docs.databricks.com/en/administration-guide/system-tables/index.html#enable-system-tables)
* Overwatch Pipeline **Cluster** must be **UC enabled** (single user and Databricks runtime version >= 11.3).
* For multi account deployment Overwatch Pipeline **Cluster** must be **UC enabled** (single user and Databricks runtime version >= 13.3).
* Overwatch latest version(0.8.0.0) should be deployed in the workspace
* Principal (user/SP) executing the Overwatch Pipeline must have access to the catalog - **system** with privileges:

  * USE CATALOG
  * USE SCHEMA 
  * SELECT
* Principal (user/SP) executing the Overwatch Pipeline must have access to the schema - **access** with privileges:
  * USE SCHEMA 
  * SELECT
* Principal (user/SP) executing the Overwatch Pipeline must have access to the table - **audit** with privileges:
  * SELECT
* Other overwatch prerequisites can be found [here](https://databrickslabs.github.io/overwatch/deployoverwatch/cloudinfra/)

Once all System Table Integration Pre-requisites are completed, please continue to [Deploy Overwatch]({{%relref "DeployOverwatch/"%}})
section.