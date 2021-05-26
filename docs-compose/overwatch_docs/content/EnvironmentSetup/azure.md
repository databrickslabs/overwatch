---
title: "Azure"
date: 2020-10-28T09:12:32-04:00
---

## Configuring Overwatch on Azure Databricks
Reach out to your Customer Success Engineer (CSE) to help you with these tasks as needed.
<br>
To get started, the [Basic Deployment](#configuring-the-event-hub-for-audit-log-delivery) configuration. 
As more modules are enabled, additional environment configuration may be required in addition to the Basic Deployment.

There are two primary sources of data that need to be configured:
* Audit Logs
  * These will be delivered through Event Hubs. The audit logs contain data for every interaction within the environment 
    and are used to track the state of various objects through time along with which accounts interacted with them. This data
    is relatively small and it certainly doesn't contain any large data sets like cluster/spark logs.
* Cluster Logs - Crucial to get the most out of Overwatch
  * These logs can get quite large and they are stored in a very inefficient format for query and long-term storage.
    This is why it's crucial to create a dedicated storage account for these and ensure TTL (time-to-live) is enabled
    to minimize long-term, unnecessary costs.
    It's not recommended to store these on DBFS directly (dbfs mount points are ok).
  * Best Practice - Multi-Workspace -- When multiple workspaces are using Overwatch within a single region it's best to
    ensure that each are going to their own prefix, even if sharing a storage account. This greatly reduces Overwatch scan times
    as the log files build up. If scan times get too long, the TTL can be reduced or additional storage accounts can 
    be created to increase read IOPS throughput (rarely necessary) intra-region.
    
![AzureClusterLogging](/images/EnvironmentSetup/Cluster_Logs_Azure.png)

| Basic Deployment       | Multi-Region Deployment |
| ---------------------- | ----------------------  |
| ![BasicAzureArch](/images/EnvironmentSetup/Overwatch_Arch_Simple_Azure.png)| ![AzureArch](/images/EnvironmentSetup/Overwatch_Arch_Azure.png)|

### Configuring the Event Hub For Audit Log Delivery
* Audit Log Delivery
    * At present the only supported method for audit log delivery is through Eventhub delivery via Azure Diagnostic Logging. 
    Overwatch will consume the events as a batch stream (Trigger.Once) once/period when the job runs. To configure 
    Eventhub to deliver these logs, follow the steps below.

### Configuing The Event Hub
#### Step 1
[Create or reuse an Event Hub namespace.](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create)
{{% notice warning %}}
**Tht Event Hub Namespace MUST be in the same location as your control plane** 
{{% /notice %}}

{{% notice info %}}
If you select the "Basic" Pricing Tier, just note that you will be required to ensure at least two successful 
Overwatch runs per day to ensure there won't be data loss. This is because message retention is 1 day meaning that 
data expires out of EH every 24 hours. "Standard" Pricing tier doesn't cost much more but 
it will give you up to 7 days retention which is much safer if you grow dependent on Overwatch reports. 
{{% /notice %}}

{{% notice info %}}
[**Throughput Units**](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-scalability#throughput-units) 
In almost all cases, 1 static throughput unit is sufficient for Overwatch. Cases where this may not be true are 
include cases where there are many users across many workspaces sharing a single EH Namespace.
Review the Throughput Units sizing and make the best decision for you.
{{% /notice %}}

Inside the namespace create an Event Hub

#### Step 2
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

#### Validate Messages Are Flowing
Now that you have configured you Overwatch EventHub Namespace and a named Event Hub inside of the namespace, it's
time to validate that messages are flowing. You may have to wait several minutes to begin to see messages flowing
depending on how busy the workspace is. There are two things that are commonly missed, please double check the
two bullet points below, there are images to help clarify as well.
* A named Event Hub has been created within the Namespace.
* Messages are flowing into the named event hub
![Named_EH_Visual](/images/EnvironmentSetup/Azure_EH_Example.png) ![EH_Validation](/images/EnvironmentSetup/EH_Validation.png)

#### Step 3
With your Event Hub Namespace and Named Event Hub created with data flowing, 
Navigate to your the Azure Databricks workspace[s] (in the portal) for which you'd like 
to enable Overwatch. Under Monitoring section --> Diagnostics settings --> Add diagnostic setting. Configure 
your log delivery similar to the example in the image below.

Additionally, ensure that the Overwatch account has sufficient privileges to read data from the Event Hub\[s\] created
above. A common method for providing Overwatch access to the Event Hub is to simply capture the connection string 
and store it as a [secret](https://docs.databricks.com/security/secrets/index.html). 
There are many methods through which to authorize Overwatch, just ensure it has the access to read from the Event Hub Stream.

{{% notice warning %}}
**DO NOT** leave **Event hub name** empty! Even though Azure doesn't require an event hub name, you must create an
event hub underneath the event hub namespace and give it a name. Reference the name here.
{{% /notice %}}

![EH_Base_Setup](/images/EnvironmentSetup/EH_BaseConfig.png)
