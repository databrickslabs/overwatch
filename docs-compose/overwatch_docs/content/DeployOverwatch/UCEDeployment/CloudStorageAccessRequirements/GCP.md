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
for detailed steps.  

### Google Service Account for Overwatch Job Cluster

After the Storage Credentials are created, the existing Service Account attached to the Overwatch Job cluster needs to
be provisioned read/write access to the storage target for the Overwatch Output 
(which will ultimately become your external location). This Service Account can be added to Overwatch Job/Interactive 
Clusters.

The following steps needs to be performed to external location’s GCS bucket permissions -
* Add Service Account which is attached to the Overwatch Job cluster
* Add Roles - 
  * Storage Admin 
  * Storage Legacy Bucket Owner

### Cluster Logging Locations Setup

{{% notice note%}}
**GCP -- Remote Cluster Logs** - Overwatch version 0.7.2.0.x  does not support cluster logs to be collected from remote
workspaces. Databricks on GCP, does not support mounted/GCS bucket locations. Customers must
provide DBFS root path as a target for log delivery. This disables Overwatch's ability to collect cluster logs from
remote workspaces at this time. A fix is in progress and expected to be available in the next release but for now,
Overwatch customers must make a deployment on each workspace to the same target. After the fix is published you will
be able to unify the configs and execute Overwatch from a single workspace and monitor remote workspaces.
{{% /notice %}}
