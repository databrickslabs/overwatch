---
title: "Security Considerations"
date: 2022-12-13T14:48:09-05:00
weight: 3
---

## API Access
Overwatch utilizes several APIs to normalize the platform data. Overwatch leverages secret scopes and keys to acquire 
a token that is authorized to access the platform. The account that owns the token (i.e. dapi token) must have 
read access to the assets you wish to manage. If the token owner is a non-admin account the account must be granted 
read level access to the assets to be monitored.

Scope | Access Required
:----------------|:-----------------------------------------
**clusters**|Can Attach To -- all clusters
**clusterEvents**|Can Attach To -- all clusters
**pools**|Read -- all Pools
**jobs**|Read -- all Jobs
**dbsql**|Read -- all Warehouses
**accounts**|Token **MUST BE ADMIN**

If the scope is not referenced above it does not require API access

## Access To The Secrets
The owner of the Overwatch Job must have read access to the secret scopes used to store any/all credentials used by 
the Overwatch pipeline.

For example, if John runs a notebook, John must have access to the secrets used in the config. The same is true if John
creates a scheduled job and sets it to run on a schedule. This changes when it goes to production though and John
sends it through production promotion process and the job is now owned by the etl-admin principle. Since the job
is now owned by the etl-admin principle, etl-admin must have access to the relevant secrets.

## Storage Access
In addition to API authentication / authorization there are some storage access that Overwatch requires.

### Audit Logs (AWS Only)
Overwatch cluster must be able to read the audit logs

### Overwatch Target Location
Overwatch cluster must be able to read/write from/to the output location (i.e. storage prefix) 

Overwatch, by default, will create a single database that, is accessible to everyone in your organization unless you
specify a location for the database in the configuration that is secured. Several of the modules
capture fairly sensitive data such as users, userIDs, etc. It is suggested that the configuration specify two
databases in the configuration:
* **ETL database** -- hold all raw and intermediate transform entities. This database can be secured
  allowing only the necessary data engineers direct access.
* **Consumer database** -- Holds views only and is easy to secure using Databricks' table ACLs (assuming no direct
  scala access). The consumer database holds only views that point to tables so additional security can easily be
  attributed at this layer. Additionally, when registering the Overwatch Databases on remote workspaces certain 
  organization_ids can be specified so that only the data for those workspaces are present in this consumer Database.

For more information on how to configure the separation of ETL and consumption databases, please reference the
[**configuration**]({{%relref "DeployOverwatch/ConfigureOverwatch/Configuration.md"%}}) page.

Additional steps can be taken to secure the storage location of the ETL entities as necessary. The method for
securing access to these tables would be the same as with any set of tables in your organization.

### Recommended Authorizations Approach
* **For AWS** -- Create an IAM Role and provision it with the following authorizations and then enable an Instance 
  Profile to utilize the IAM Role for the cluster
  * read/write access to the [Overwatch Output Storage](#overwatch-target-location)
  * read access to all locations where cluster logs are stored
* **For Azure** -- Create an SPN and provision it with and then add the configuration [below](#azure-storage-auth-config) 
  to authorize the cluster to use the SPN to access the storage locations.
  * read/write access to the [Overwatch Output Storage](#overwatch-target-location)
  * read access to all locations where cluster logs are stored

## Event Hub Access (AZURE ONLY)
In Azure the audit logs must be acquired through an Event Hub stream. The details for configuring and provisioning 
access are detailed in the 
[Azure Cloud Infrastructure]({{%relref "DeployOverwatch/CloudInfra/Azure"%}}/#audit-log-delivery-via-event-hub) Section


##### Azure Storage Auth Config 
Fill out the following and add it to your cluster as a spark config. For more information please reference 
the [Azure DOCS](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts#--mount-adls-gen2-or-blob-storage-with-abfs)

Note that there are two sets of configs, both are required as both spark on the executors and the driver must be 
authorized to the storage.
```
fs.azure.account.auth.type OAuth
fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
fs.azure.account.oauth2.client.id <application-id>
fs.azure.account.oauth2.client.secret {{secrets/<SCOPE_NAME>/<KEY_NAME>}}
fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/<directory-id>/oauth2/token

spark.hadoop.fs.azure.account.auth.type OAuth
spark.hadoop.fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
spark.hadoop.fs.azure.account.oauth2.client.id <application-id>
spark.hadoop.fs.azure.account.oauth2.client.secret {{secrets/<SCOPE_NAME>/<KEY_NAME>}}
spark.hadoop.fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/<directory-id>/oauth2/token
```

