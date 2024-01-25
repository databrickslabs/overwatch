---
title: "Cluster Configuration"
date: 2022-12-13T17:01:49-05:00
weight: 1
---

## Cluster Requirements
* DBR 11.3LTS as of 0.7.1.0
    * Overwatch will likely run on different versions of DBR but is built and tested on 11.3LTS since 0.7.1
    * Overwatch < 0.7.1 -- DBR 10.4LTS
    * Overwatch < 0.6.1 -- DBR 9.1LTS
* Using Photon
    * As of 0.7.1.0 **Photon is recommended** so long as the Overwatch cluster is using DBR 11.3LTS+. 
      Photon does increase the DBU spend but the performance boost often results
      in the code running significantly more efficiently netting out a benefit. Mileage can vary between customers so 
      if you really want to know which is most efficient, feel free to run on both and use Overwatch to determine which is 
      best for you.
    * Prior to 0.7.1.0 and DBR 11.3LTS Photon was untested
* Disable Autoscaling - See [Notes On Autoscaling](#notes-on-autoscaling)
    * External optimize cluster recommendations are different.
      See [External Optimize]({{%relref "DataEngineer/AdvancedTopics.md"%}}/#externalize-optimize--z-order-as-of-060) for more details
* Add the relevant [dependencies](#cluster-dependencies)

### Cluster Dependencies
Add the following dependencies to your cluster
* Overwatch Assembly (fat jar): `com.databricks.labs:overwatch_2.12:<latest>`
* **(Azure Only, if not using System Tables)** azure-eventhubs-spark - integration with Azure EventHubs
    * Maven Coordinate: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21`
* **(Azure Only - With AAD Auth For EH, if not using system tables)** msal4j - library to support AAD Authorization
  * Maven Coordinate: `com.microsoft.azure:msal4j:1.10.1`
* If **maven isn't accessible** in your environment you can compile an uber jar with all dependencies
  * Download the [**uber_pom.xml**](/assets/DeployOverwatch/pom.xml) and run `mvn clean package` from within the same directory as the pom.xml
    * This method does require you have maven installed and configured correctly

### Cluster Config Recommendations
* Azure
    * Node Type (Driver & Worker) - Standard_D16s_v3
        * Use n Standard_E*d[s]_v4 for workers for historical loads and Photon
        * Large / Very Large workspaces may see a significant improvement using Standard_E16d[s]_v4 workers but mileage varies, cost/benefit analysis required
    * Node Count - 2
        * This may be increased if necessary but note that bronze is not linearly scalable; thus, increasing core count
          may not improve runtimes. Please see [Optimizing Overwatch]({{%relref "DataEngineer/AdvancedTopics.md"%}}/#optimizing-overwatch) for more information.
* AWS
    * Node Type (Driver & Worker) - R5d.4xlarge
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