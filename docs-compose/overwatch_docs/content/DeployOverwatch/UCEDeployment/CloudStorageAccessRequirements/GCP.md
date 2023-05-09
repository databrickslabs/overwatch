---
title: "GCP"
date: 2023-04-18T11:28:39-05:00
weight: 3
---

Below are the requirement needed for Storage Access setup in GCP

* [Google Service Account for Storage Credentials](#google-service-account-for-storage-credentials)

* [Google Service Account for Overwatch Job Cluster](#google-service-account-for-overwatch-job-cluster)

* [Cluster Logging Locations Setup](#cluster-logging-locations-setup) 


### Google Service Account for Storage Credentials

Unity Catalog Storage credentials  should have the ability to read and write to a External Location(GCS bucket) by 
assigning appropriate IAM roles on that bucket to a Databricks-generated Google Cloud service account. Please refer the 
[docs](https://docs.gcp.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#manage-storage-credentials) 
for a detailed steps.  

### Google Service Account for Overwatch Job Cluster

After the Storage Credentials is created, the existing Service Account attached to the Overwatch Job cluster needs to
be provisioned read/write access to the storage target for the Overwatch Output 
(which will ultimately become your external location). This Service Account can be added to Overwatch Job/Interactive 
Clusters.

Following steps needs to be performed to external location’s GCS bucket permissions -
* Add Service Account which is attached to the Overwatch Job cluster
* Add Roles - 
  * Storage Admin 
  * Storage Legacy Bucket Owner

### Cluster Logging Locations Setup

{{% notice note%}}
 _Latest Overwatch version does not support clusterlogs to be collected from multiple workspaces.
  As per the latest Databricks GCP release, cluster log location does not support mounted/GCS bucket location, we have to
  provide DBFS root as a target for log delivery. This can cause issues while populating spark job related tables in
  multi workspace deployment. Keep watching this space for more updates as Overwatch team is working on a fix for this issues_
{{% /notice %}}
