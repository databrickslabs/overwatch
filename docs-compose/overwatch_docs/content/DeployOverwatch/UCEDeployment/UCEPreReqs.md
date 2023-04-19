---
title: "UC Pre-Requisites"
date: 2023-04-18T11:28:39-05:00
weight: 2
---

## Unity Catalog Prerequisites

The following pre-requisites are in addition to the pre-requisites documented in the
[UC Storage Requirements]({{%relref "DeployOverwatch/UCEDeployment/CloudStorageAccessRequirements/"%}}). After 
all UC Pre-requisites are completed, please continue to [Deploy Overwatch]({{%relref "DeployOverwatch/"%}}) section.

This section will walk you through the steps necessary as a prerequisite to deploy Overwatch on Unity Catalog.
* **Workspace** should be **UC enabled**.
* Overwatch Pipeline **Cluster** must be **UC enabled** (single user and runtime version > 11.3+).
* Create **Storage Credentials** to be used by the external locations provisioned with appropriate read/write 
  access to the UC External Location
  ([AWS](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#create-a-storage-credential) | 
  [GCP](https://docs.gcp.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#manage-storage-credentials) | 
  [AZURE](https://learn.microsoft.com/en-gb/azure/databricks/data-governance/unity-catalog/manage-external-locations-and-credentials#--create-a-storage-credential)) 
  with privileges:
  * READ FILES
  * WRITE FILES
  * CREATE EXTERNAL TABLE
* Create **UC External location** where Overwatch data is to be stored
  ([AWS](https://docs.databricks.com/data/manage-external-locations.html#manage-unity-catalog-external-locations-in-data-explorer) | 
  [GCP](https://docs.gcp.databricks.com/data/manage-external-locations.html#manage-unity-catalog-external-locations-in-data-explorer) | 
  [AZURE](https://learn.microsoft.com/en-gb/azure/databricks/data/manage-external-locations#create-external-location)). 
  Provide ALL PRIVILEGES permission to the principal (user/SP) that is going to run the Overwatch Pipeline.
* Create a **Catalog** or identify an existing catalog where overwatch data will be stored. 
  **Overwatch** code **WILL NOT** create the catalog, it must be pre-existing.
* Principal (user/SP) executing the Overwatch Pipeline must have access to the **catalog** with **privileges**:
  * USE CATALOG
  * USE SCHEMA
  * SELECT
* **Create ETL and Consumer Schemas** (i.e. databases). Overwatch **WILL NOT** create the Schemas in a UC Deployment. 
  Principal (user/SP) executing the Overwatch Pipeline must have the following privileges on the Schema AND must be 
  an Owner of the Schema.
  * IS OWNER -- required since schema metadata is edited and requires schema ownership
    * The schema owner can be a user, service principal, or group.
  * USE SCHEMA
  * CREATE TABLE
  * MODIFY
  * SELECT
* Overwatch latest version(0.7.1.0+) should be deployed in the workspace
* Other overwatch prerequisites can be found [here](https://databrickslabs.github.io/overwatch/deployoverwatch/cloudinfra/)

## SQL Command to Grant Permissions to various UC Objects
The following can be done through the UI or via commands as shown below

### SQL Command to grant permissions on Storage Credentials
```sql
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON STORAGE CREDENTIAL `<storage-credential-name>` TO `<principal>`;
```

### SQL Command to grant permissions on External Locations
```sql
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `<external-location-name>` TO `<principal>`;
```

### SQL Command to grant permissions on Catalog
```sql
GRANT USE CATALOG, USE SCHEMA, SELECT
  ON CATALOG <catalog-name>
  TO `<principal>`
```

### SQL Command to grant permissions on ETL Database
```sql
GRANT USE SCHEMA, CREATE TABLE, MODIFY
ON SCHEMA <catalog-name>.<etl-database>
TO `<principal>`;
```

### SQL Command to grant permissions on Consumer Database
```sql
GRANT USE SCHEMA, CREATE TABLE, MODIFY
ON SCHEMA <catalog-name>.<consumer-database>
TO `<principal>`;
```
