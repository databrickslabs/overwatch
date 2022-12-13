---
title: "Cluster Configuration"
date: 2022-12-13T17:01:49-05:00
weight: 1
---

## Cluster Requirements
* DBR 10.4LTS as of 0.6.1
    * Overwatch will likely run on later versions but is built and tested on 10.4LTS since 0.6.1
    * Overwatch < 0.6.1 -- DBR 9.1LTS
    * Overwatch version 0.6.1+ -- DBR 10.4LTS
* Do not use Photon
    * Photon optimization will begin with DBR 11.x LTS please wait on photon
* Disable Autoscaling - See [Notes On Autoscaling](#notes-on-autoscaling)
    * External optimize cluster recommendations are different.
      See [External Optimize]({{%relref "DataEngineer/AdvancedTopics.md"%}}/#externalize-optimize--z-order-as-of-060) for more details
* Add the relevant [dependencies](#cluster-dependencies)

### Cluster Dependencies
Add the following dependencies to your cluster
* Overwatch Assembly (fat jar): `com.databricks.labs:overwatch_2.12:<latest>`
* **(Azure Only)** azure-eventhubs-spark - integration with Azure EventHubs
    * Maven Coordinate: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21`

### Cluster Config Recommendations
* Azure
    * Node Type (Driver & Worker) - Standard_D16s_v3
        * Use n Standard_E*d[s]_v4 for workers for historical loads
        * Large / Very Large workspaces may see a significant improvement using Standard_E16d[s]_v4 workers but mileage varies, cost/benefit analysis required
    * Node Count - 2
        * This may be increased if necessary but note that bronze is not linearly scalable; thus, increasing core count
          may not improve runtimes. Please see [Optimizing Overwatch]({{%relref "DataEngineer/AdvancedTopics.md"%}}/#optimizing-overwatch) for more information.
* AWS
    * Node Type (Driver & Worker) - R5.4xlarge
        * Use n i3.*xlarge for workers for historical loads
        * Large / Very Large workspaces may see a significant improvement using i3.4xlarge workers but mileage varies, cost/benefit analysis required
    * Node Count - 2
        * This may be increased if necessary but note that bronze is not linearly scalable; thus, increasing core count
          may not improve runtimes. Please see [Optimizing Overwatch]({{%relref "DataEngineer/AdvancedTopics.md"%}}/#optimizing-overwatch) for more information.

### Notes On Autoscaling
* Auto-scaling compute -- **Not Recommended**
    * Note that autoscaling compute will not be extremely efficient due to some of the compute tails
      as a result of log file size skew and storage mediums. Additionally, some modules require thousands of API calls
      (for historical loads) and have to be throttled to protect the workspace.
* Auto-scaling Local Storage -- **Strongly Recommended** for historical loads
    * Some of the data sources can grow to be quite large and require very large shuffle stages which requires
      sufficient local disk. If you choose not to use auto-scaling storage be sure you provision sufficient local
      disk space.
        * SSD or NVME (preferred) -- It's strongly recommended to use fast local disks as there can be very large shuffles