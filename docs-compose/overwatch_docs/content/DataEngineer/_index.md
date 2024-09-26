---
title: "Data Engineering"
weight: 3
---

This section is meant for analysts and Data Engineers looking for more detail on managing the Overwatch 
Data Ingestion Process and also contains the data dictionary for all columns delivered to the presentation layer.

* [Data Dictionary (Latest)]({{%relref "dataengineer/definitions/_index.md"%}})
    * [0.7.1.x]({{%relref "dataengineer/definitions/071x.md"%}})
    * [0.6.1.x]({{%relref "dataengineer/definitions/061x.md"%}})
    * [0.5.x](/images/_index/Overwatch_Gold_05x.png)
* [Overwatch Pipeline Details & Management]({{%relref "dataengineer/Pipeline_Management.md"%}})
  * [Overview]({{%relref "dataengineer/Pipeline_Management.md"%}}#overwatch-data-promotion-process)
  * [Ingesting & Incremental Loading]({{%relref "dataengineer/Pipeline_Management.md"%}}#data-ingestion-and-resume-process)
  * [Pipeline Report (PipReport)]({{%relref "dataengineer/Pipeline_Management.md"%}}#the-pipeline-report)
  * [Overwatch Pipeline Time]({{%relref "dataengineer/Pipeline_Management.md"%}}#overwatch-time)
  * [Module Flow & Dependencies]({{%relref "dataengineer/Pipeline_Management.md"%}}#module-dependencies)
  * [Reloading Data]({{%relref "dataengineer/Pipeline_Management.md"%}}#reloading-data)
  * [Executing Specific Pipelines (i.e. Bronze/Silver/Gold)]({{%relref "dataengineer/Pipeline_Management.md"%}}#running-only-specific-layers)
* [Upgrading Overwatch - Overview]({{%relref "dataengineer/Upgrade.md"%}})
* [Productionizing Overwatch]({{%relref "dataengineer/Productionizing.md"%}})
  * [Unifying Cluster Logs]({{%relref "dataengineer/Productionizing.md"%}}#cluster-logging)
  * [Bronze Backup]({{%relref "dataengineer/Productionizing.md"%}}#backups)
  * [Alerting On Module Failures]({{%relref "dataengineer/Productionizing.md"%}}#alerting-on-failures)
  * [Externalize Optimize & Z-Order]({{%relref "dataengineer/Productionizing.md"%}}#ensure-externalize-optimize-is-enabled)


## Overwatch ERD
![OverwatchERD](/images/_index/Overwatch_Gold_0.8.2.0.png)
For full fidelity image, right click and save the image.