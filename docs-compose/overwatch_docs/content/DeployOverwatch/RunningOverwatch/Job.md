---
title: "Job"
date: 2022-12-12T12:04:53-05:00
draft: true
weight: 2
---

## This section will walk you through the steps necessary to deploy Overwatch through Job.

### Main Class
The main class for job is `com.databricks.labs.overwatch.MultiWorkspaceRunner`<br>

### Dependent Library
`com.databricks.labs:overwatch_2.12:0.7.0.1.x`<br> and above

`com.microsoft.azure:azure-eventhubs-spark_2.12:2.*.*`<br> and above(For Azure only)

### Parameters

**Job takes 3 arguments** 
* Args(0): Path of Config.csv.
* Args(1): Number of threads to complete the task in parallel.
* Args(2): Deployment zone, it must be either "Bronze", "Silver", "Gold" to deploy a particular zone or "Bronze,Silver,Gold" to deploy all the zones.


![newUIJarSetup](/images/GettingStarted/mswjob.png)

