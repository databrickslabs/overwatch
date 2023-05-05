---
title: "GCP"
date: 2023-04-18T11:28:39-05:00
weight: 3
---

## Creating Google Service Account for Storage Credentials
Unity Catalog Storage credentials  should have the ability to read and write to a External Location(GCS bucket) by assigning IAM roles on that bucket to a Databricks-generated Google Cloud service account.
Please refer the [docs](https://docs.gcp.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#manage-storage-credentials) for a detailed walkthrough 

## Change in existing Service Account attached to Overwatch Job Cluster

After the Storage Credentials is created, the existing Service Account attached to the Overwatch Job cluster needs to be provisioned read/write access to the storage target for the Overwatch Output (which will ultimately become your external location).

Following steps needs to be performed to external location’s GCS bucket permissions -
* Add Service Account which is attached to the Overwatch Job cluster
* Add Roles - 
  * Storage Admin 
  * Storage Legacy Bucket Owner

## Cluster Logging Locations
