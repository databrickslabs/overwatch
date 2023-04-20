---
title: "Migrating To UC"
date: 2023-04-18T11:28:39-05:00
weight: 4
---

## Migrating Existing Deployment From Hive_Metastore To UC

Migrating a deployment from Hive Metastore has been made very simple. The steps are
1. Complete the [UC Pre-Requisites]({{%relref "/DeployOverwatch/UCEDeployment/UCEPreReqs.md"%}})
2. Ensure [storage is set up correctly]({{%relref "/DeployOverwatch/UCEDeployment/CloudStorageAccessRequirements/"%}})
3. Update the [Overwatch Configuration appropriately for UC]({{%relref "/DeployOverwatch/UCEDeployment/UCEConfiguration.md"%}})
4. Use the Migration Notebook below to migrate the data from Hive to UC
5. Resume the job

### Migration Notebook 

* **Migration Notebook** ( [**HTML**](/assets/DeployOverwatch/UC_Migration_Utility-Hive_Metastore_To_Unity_Catalog.html) | 
  [**DBC**](/assets/DeployOverwatch/UC_Migration_Utility-Hive_Metastore_To_Unity_Catalog.dbc) )
    * Details to Run are in the notebook


