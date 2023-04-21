---
title: "Azure"
date: 2023-04-18T11:28:39-05:00
weight: 2
---
## Creating the Managed Identity
[Create a Managed Identity](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/azure-managed-identities#--configure-a-managed-identity-for-unity-catalog) 
to authorize access to the external location. This managed Identity will be configured using a [Databricks 
Storage Credential](https://learn.microsoft.com/en-us/azure/databricks/data/manage-storage-credentials). Databricks 
recommends using an Access Connector for Azure Databricks.

After the managed identity is created, it needs to be provisioned read/write access to the storage target for the 
Overwatch Output (which will ultimately become your external location).

## Provisioning the Managed Identity to The Storage
If you intend to provision the managed identity **to the storage account** you need to grant the managed identity 
* Storage Blob Data Contributor

If you intend to provision the managed identity **to a specific container** you need to grant the managed identity
* Storage Blob Data Contributor
* Storage Blob Delegator

![StorageACRoles](/images/EnvironmentSetup/storage_ac_roles.png)