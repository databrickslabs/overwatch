## Unity Catalog Prerequisites

This section will walk you through the steps necessary as a prerequisite to deploy Overwatch on Unity Catalog.
* Workspace should be UC enabled.
* Clusters running Overwatch code/job should be UC enabled(single user and runtime version > 11).
* Create Storage Credentials to be used by the external locations with appropriate read/write access to the UC External Location [AWS](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#create-a-storage-credential), [GCP](https://docs.gcp.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#manage-storage-credentials), [AZURE](https://learn.microsoft.com/en-gb/azure/databricks/data-governance/unity-catalog/manage-external-locations-and-credentials#--create-a-storage-credential) with privileges - READ FILES, WRITE FILES, CREATE EXTERNAL TABLE
* Create UC External location where overwatch data needs to be stored [AWS](https://docs.databricks.com/data/manage-external-locations.html#manage-unity-catalog-external-locations-in-data-explorer), [GCP](https://docs.gcp.databricks.com/data/manage-external-locations.html#manage-unity-catalog-external-locations-in-data-explorer), [AZURE](https://learn.microsoft.com/en-gb/azure/databricks/data/manage-external-locations#create-external-location). Provide ALL PRIVILEGES permission to the owner of the Overwatch Job or user running the overwatch job.
* Create a Catalog or select an existing Catalog where overwatch data will be stored before running Overwatch Job. Overwatch code will not be responsible for creating the catalog.
* Users running overwatch should have access to the Catalog with privileges - USE CATALOG, USE SCHEMA, SELECT
* Create ETL and Consumer schemas (i.e. databases). Overwatch job will not create the Schemas in a UC Deployment. Provide privileges - USE SCHEMA, CREATE TABLE, MODIFY, SELECT to the owner/ user running the overwatch job.
* Direct access to external location’ storage | [DOCS]({{%relref "/DeployOverwatch/UCEDeployment/CloudStorageAccessRequirements/_index.md"%}})
* Overwatch latest version(0.7.1.0+) should be deployed in the workspace
* Other overwatch prerequisites can be found [here](https://databrickslabs.github.io/overwatch/deployoverwatch/cloudinfra/)
