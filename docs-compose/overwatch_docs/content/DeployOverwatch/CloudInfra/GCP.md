---
title: "GCP"
date: 2023-09-07
---

## Configuring Overwatch on GCP - Databricks
Reach out to your Databricks representative to help you with these tasks as needed.

There are two primary sources of data that need to be configured:
* [Audit Logs-GCP](https://docs.gcp.databricks.com/administration-guide/account-settings/audit-logs.html)
    * These will be delivered to the configured bucket. These buckets are configured on a per-workspace basis
      and can be delivered to the same target bucket, just ensure that the prefixes are different to avoid collisions.
      We don't want multiple workspaces delivering into the same prefix. The audit logs contain data for every interaction
      within the environment and are used to track the state of various objects through time along with which accounts
      interacted with them. This data is relatively small and delivery occurs infrequently which is why it's
      rarely of any consequence to deliver audit logs to buckets even outside of the control plane region.
* Cluster Logs - Crucial to get the most out of Overwatch
    * Cluster logs delivery location is configured in the cluster spec --> Advanced Options --> Logging. These logs can
      get quite large and they are stored in a very inefficient format for query and long-term storage. As such, it's
      crucial to store these logs in the same region as the worker nodes for best results. Additionally, using dedicated
      buckets provides more flexibility when configuring TTL (time-to-live) to minimize long-term, unnecessary costs.
      It's not recommended to store these on DBFS directly (dbfs mount points are ok).
    * Best Practice - Multi-Workspace -- When multiple workspaces are using Overwatch within a single region it's best to
      ensure that each are going to their own prefix, even if sharing a bucket. This greatly reduces Overwatch scan times
      as the log files build up.
    * To enable Cluster Logs on Multiworkspace - follow this [link]({{%relref "DeployOverwatch/UCEDeployment/CloudStorageAccessRequirements/gcp"%}}/#cluster-logging-locations-setup)

{{% notice note%}}
**GCP -- Remote Cluster Logs** - Databricks on GCP, does not support mounted/GCS bucket locations. Customers must
provide DBFS root path as a target for log delivery.
{{% /notice %}}


## Reference Architecture
As of 0.7.1 Overwatch can be deployed on a single workspace and retrieve data from one or more workspaces. For more details
on requirements see [Multi-Workspace Consideration]({{%relref "DeployOverwatch"%}}/#multi-workspace-monitoring---considerations).
There are many cases where some workspaces should be able to monitor many workspaces and others should only monitor
themselves. Additionally, co-location of the output data and who should be able to access what data also comes into play,
this reference architecture can accommodate all of these needs. To learn more about the details walk through the
[deployment steps]({{%relref "DeployOverwatch"%}})
![AzureArch](/images/EnvironmentSetup/Overwatch_Arch_AWS.png)
