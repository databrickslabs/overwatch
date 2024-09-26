---
title: "UC Configuration Details"
date: 2023-04-18T11:28:39-05:00
weight: 3
---

## Configuration changes required for UC Enablement

There is no unique configuration process for UC; however, the format for the **THREE CONFIGURATIONS** below 
are specific for UC Enablement. For all other configurations, please follow the 
[Configuration]({{%relref "DeployOverwatch/ConfigureOverwatch/Configuration.md"%}})

* **etl_database_name** - <catalog_name>.<etl_database_name>
* **consumer_database_name** - <catalog_name>.<consumer_database_name>
* **storage_prefix** -  <UC External Location\>/<storage_prefix>


{{% notice note %}}
With the release of Overwatch 0.8.2.0, we have added support for Multi Catalog Overwatch Deployment.
Now user can provide different catalogs for ETL Database and Consumer Database.
{{% /notice %}}