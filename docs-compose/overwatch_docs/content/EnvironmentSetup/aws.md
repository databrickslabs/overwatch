---
title: "AWS"
date: 2020-10-28T09:12:28-04:00
draft: true
---

## Configuring Overwatch on AWS - Databricks
### Historical / Batch Environment Configuration
Reach out to your Customer Success Engineer (CSE) to help you with these tasks as needed.
<br>
To get started, the [Basic Deployment](#basic-deployment) configuration. As more modules are enabled, additional
environment configuration may be required in addition to the Basic Deployment.

#### Basic Deployment
* [Audit Log Delivery](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html)
* Create Service User and/or Token for Overwatch to use
    * Grant required privileges to the Overwatch token
    * Register the token secret as a [Databricks Secret](https://docs.databricks.com/security/secrets/index.html)
{{% notice warning %}}
If a secret is not created and defined the user that owns the process must have the necessary permissions to 
perform all Overwatch tasks.
{{% /notice %}}
    
#### With IAM Passthrough Security Capture
{{% notice info %}}
Targeted Q1 2021 -- See [Roadmap]({{%relref "GettingStarted/Roadmap.md"%}})
{{% /notice %}}

#### With Databricks Billable Usage Delivery Logs
Detailed costs data 
[directly from Databricks](https://docs.databricks.com/administration-guide/account-settings/billable-usage-delivery.html). 
This data can significantly enhance deeper level cost metrics. Even though Overwatch doesn't support this just yet, 
if you go ahead and configure the delivery of these reports, when Overwatch begins supporting it, it will be able
to load all the historical data from the day that you began receiving it. 
{{% notice info %}}
Targeted Q1 2021 -- See [Roadmap]({{%relref "GettingStarted/Roadmap.md"%}})
{{% /notice %}}

#### With Cloud Provider Costs
There can be substantial cloud provider costs associated with a cluster. Databricks (and Overwatch) has no visibility 
to this by default. The plan here is to work with a few pilot customers to build API keys with appropriate 
permissions to allow Overwatch to capture the compute, storage, etc costs associated with cluster machines. The 
primary focus here would be to help customers visualize and optimize SPOT markets by illuminating costs 
and opportunities across different AZs in your region given you machine types. Additionally, Overwatch could 
make recommendations on:
* Actual compute/disk costs associated with clusters
* Optimal time of day for large jobs
* Small adjustments to node type by hour of day (i.e. i3.4xlarge instead of i3.2xlarge)

The goal is to go even deeper than compute by looking at disk IO as well to determine the benefit/need of NVMEs 
vs SSDs vs HDDs. Picking the right storage type and amount can save large sums over the course of a year. Picking 
the wrong type (or not enough) can result in elevated compute bills due to inefficiencies. If you're interested in 
this feature and would like to participate, please inform your CSE and/or Databricks Account Team.

{{% notice info %}}
Targeted 2021 -- See [Roadmap]({{%relref "GettingStarted/Roadmap.md"%}})
{{% /notice %}}

#### Realtime Enablement
{{% notice info %}}
2021 Feature Release -- See [Roadmap]({{%relref "GettingStarted/Roadmap.md"%}})
{{% /notice %}}