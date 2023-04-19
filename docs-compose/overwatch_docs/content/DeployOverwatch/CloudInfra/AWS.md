---
title: "AWS/GCP"
date: 2022-12-12T11:29:56-05:00
---

## Configuring Overwatch on AWS/GCP - Databricks
Reach out to your Customer Success Engineer (CSE) to help you with these tasks as needed.
<br>
To get started, the [Basic Deployment](#basic-deployment) configuration. As more modules are enabled, additional
environment configuration may be required in addition to the Basic Deployment.

There are two primary sources of data that need to be configured:
* [Audit Logs-AWS](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html)/[Audit Logs-GCP](https://docs.gcp.databricks.com/administration-guide/account-settings/audit-logs.html)
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
    * Best Practice - Sharding -- When many clusters send their logs to the same prefix it reduces the AWS read parallelism
      for these small files. It's best to find a methodology for log prefixes that works for your org. One example would
      be to have each workspace and each team store their logs in a specific prefix, etc to break up the number of log
      files buried inside a single prefix. Overwatch will do all the work to aggregate these logs and deliver the data
      to the data model.

## Reference Architecture
As of 0.7.1 Overwatch can be deployed on a single workspace and retrieve data from all workspaces. For more details
on requirements see [Multi-Workspace Consideration]({{%relref "DeployOverwatch"%}}/#multi-workspace-monitoring---considerations).
There are many cases where some workspaces should be able to monitor many workspaces and others should only monitor
themselves. Additionally, co-location of the output data and who should be able to access what data also comes into play,
this reference architecture can accommodate all of these needs. To learn more about the details walk through the
[deployment steps]({{%relref "DeployOverwatch"%}})
![AzureArch](/images/EnvironmentSetup/Overwatch_Arch_AWS.png)

## Reference Architecture (Legacy)
The legacy architecture method (pre 0.7.1.0) required that Overwtach be deployed as a job on all workspaces. Since
0.7.1 this is no longer the case. See [Reference Architecture](#reference-architecture)
![AWSClusterLogging](/images/EnvironmentSetup/Cluster_Logs_AWS.png)

| Basic Deployment       | Multi-Region Deployment |
| ---------------------- | ----------------------  |
| ![BasicAwsArch](/images/EnvironmentSetup/Overwatch_Arch_Simple_AWS.png)| ![AWSArch](/images/EnvironmentSetup/Overwatch_Arch_AWS_Legacy.png)|

## With Databricks Billable Usage Delivery Logs
Detailed costs data
[directly from Databricks](https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html).
This data can significantly enhance deeper level cost metrics. Even though Overwatch doesn't support this just yet,
if you go ahead and configure the delivery of these reports, when Overwatch begins supporting it, it will be able
to load all the historical data from the day that you began receiving it.

In the meantime, it's easy to join these up with the Overwatch data for cost validation and / or exact cost break
down by dimensions not supported in the usage logs.

