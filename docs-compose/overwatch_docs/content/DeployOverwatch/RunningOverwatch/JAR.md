---
title: "As A JAR"
date: 2022-12-13T16:09:01-05:00
weight: 3
---

## This section will walk you through the steps necessary to deploy Overwatch through Job.

### Main Class
The main class for job is `com.databricks.labs.overwatch.MultiWorkspaceRunner`<br>

### Dependent Library
`com.databricks.labs:overwatch_2.12:0.7.0.1.x`<br> and above

`com.microsoft.azure:azure-eventhubs-spark_2.12:2.*.*`<br> and above(**Azure only**)

### Parameters

**Job can take upto 3 arguments**
* Args(0): Path of Config.csv.(**Mandatory** )
* Args(1): Number of threads to complete the task in parallel, 4 is the default value.(Optional)
* Args(2): Target Pipeline,the pipeline for which the deployment will be performed, if you want to perform deployment for a particular pipeline then it also can be given as
  "Bronze" or "Silver" or "Gold" or "Bronze,Silver" or "Silver,Gold" , default value is "Bronze,Silver,Gold" (Optional)
         

![newUIJarSetup](/images/GettingStarted/mswjob.png)