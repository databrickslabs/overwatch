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

The following steps needs to be performed in external location’s GCS bucket permissions -
* Go to the `Permissions` tab of the external location’s GCS bucket
* Click on `Grant Access`
* Add Service Account which is attached to the Overwatch Job cluster in the `Add Principal` section
* Add Service Account which is attached to the Overwatch Job cluster
* Add Roles - 
  * Storage Admin 
  * Storage Legacy Bucket Owner

### Cluster Logging Locations Setup
When you create a workspace, Databricks on Google Cloud creates two Google Cloud Storage GCS buckets in your GCP project:
* `databricks-<workspace-id>` - stores system data that is generated as you use various Databricks features.
This bucket includes notebook revisions, job run details, command results, and Spark logs
* `databricks-<workspace-id>-system` - contains workspace’s root storage for the Databricks File System (DBFS). 
Your DBFS root bucket is not intended for storage of production data.

Follow the [databricks-docs](https://docs.gcp.databricks.com/administration-guide/workspace/create-workspace.html#secure-the-workspaces-gcs-buckets-in-your-project) 
to get more information on these buckets. 

In order to fetch the cluster logs of the remote workspace, cluster should have access to the GCS bucket - 
`databricks-<workspace-id>`. This GCS Bucket is created in the Google Cloud project that hosts your Databricks workspace.

The following steps needs to be performed in `databricks-<workspace-id>` GCS bucket permissions -
* Go to the `Permissions` tab of the `databricks-<workspace-id>` GCS bucket
* Click on `Grant Access`
* Add Service Account which is attached to the Overwatch Job cluster in the `Add Principal` section
* Add Roles -
  * Storage Admin
  * Storage Legacy Bucket Owner

{{% notice note%}}
**GCP -- Remote Cluster Logs** - Databricks on GCP, does not support mounted/GCS bucket locations. Customers must
provide DBFS root path as a target for log delivery.
{{% /notice %}}
