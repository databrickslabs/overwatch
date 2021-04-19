---
title: "Azure"
date: 2020-10-28T09:12:32-04:00
draft: true
---

## Configuring Overwatch on Azure Databricks
### Historical / Batch Environment Configuration
Reach out to your Customer Success Engineer (CSE) to help you with these tasks as needed.
<br>
To get started, the [Basic Deployment](#basic-deployment) configuration. As more modules are enabled, additional
environment configuration may be required in addition to the Basic Deployment.

#### Basic Deployment
* Audit Log Delivery
    * At present the only supported method for audit log delivery is through Eventhub delivery via Azure Diagnostic Logging. 
    Overwatch will consume the events as a batch stream (Trigger.Once) once/period when the job runs. To configure 
    Eventhub to deliver these logs, follow the steps below.
    
##### Step 1
[Create or reuse an Event Hub namespace.](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create)
{{% notice warning %}}
**Tht Event Hub Namespace MUST be in the same location as your control plane** 
{{% /notice %}}

{{% notice info %}}
If you select the "Basic" Pricing Tier, just noted that you will be required to ensure at least two successful 
Overwatch runs per day to ensure there won't be data loss. This is because message retention is 1 day meaning that 
data expires out of EH every 24 hours. "Standard" Pricing tier doesn't cost much more but 
it will give you up to 7 days retention which is much safer if you grow dependent on Overwatch reports. 
{{% /notice %}}

{{% notice info %}}
[**Throughput Units**](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-scalability#throughput-units) 
In almost all cases, 1 static throughput unit is sufficient for Overwatch. Cases where this may not be true are 
include cases where there are many users globally across many workspaces all moving through a single EH Namespace.
Review the Throughput Units sizing and make the best decision for you.
{{% /notice %}}

Inside the namespace create an Event Hub

##### Step 2
Create an Event Hub inside your chosen (or created) EH Namespace.
{{% notice info %}}
[**Partitions**](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-scalability#partitions)
The maximum number of cores (parallelism) that can simultaneously pull data from your event hub == EH partitions.
In most cases this should be set to 32. It can be fewer, just be sure that Overwatch uses an autoscaling cluster
to minimize costs while waiting to pull the data from EH. For example, if Overwatch is pulling 30 million events 
but only has 8 partitions, any worker cores over 8 will sit idle while all 30 million events are retrieved. In a 
situation like this, the minimum autoscaling compute size should approximately equal the number of partitions to 
minimize waste.  
{{% /notice %}}

##### Step 3
With your Event Hub created and ready to go, Navigate to your the Azure Databricks workspace[s] for which you'd like 
to enable Overwatch. Under Monitoring section --> Diagnostics settings --> Add diagnostic setting. Configure 
your log deliver at least to the following spec.
![EH_Base_Setup](/images/EnvironmentSetup/EH_BaseConfig.png)


* Create Service User and/or Token for Overwatch to use
    * Grant required privileges to the Overwatch token
    * Register the token secret as a [Databricks Secret](https://docs.databricks.com/security/secrets/index.html)
{{% notice warning %}}
If a secret is not created and defined the user that owns the process must have the necessary permissions to 
perform all Overwatch tasks.
{{% /notice %}}
    
#### With AD Passthrough Security Capture
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
permissions to allow Overwatch to capture the compute, storage, network, disk, etc costs associated with machines. 
On Azure it's likely we work in tandem with LogAnalytics to some extent to build a repeatable solution to easily  
opt-in to this feature. If you're interested in this feature and would like to participate, please inform your CSE 
and/or Databricks Account Team.

{{% notice info %}}
Targeted 2021 -- See [Roadmap]({{%relref "GettingStarted/Roadmap.md"%}})
{{% /notice %}}

#### Realtime Enablement
{{% notice info %}}
2021 Feature Release -- See [Roadmap]({{%relref "GettingStarted/Roadmap.md"%}})
{{% /notice %}}