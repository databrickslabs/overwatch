---
title: "As A JAR"
date: 2022-12-13T16:09:01-05:00
weight: 3
---

## Deploying Overwatch As A JAR On Databricks Workflows
**This deployment method requires Overwatch Version 0.7.1.0+**  

### Main Class
The main class for job is `com.databricks.labs.overwatch.MultiWorkspaceRunner`<br>

### Dependent Library
`com.databricks.labs:overwatch_2.12:0.7.1.x`

`com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21` **(Azure only)**

### Parameters

**Job can take upto 3 arguments**
* Args(0): Path of Config.csv **(Mandatory)**
  * EX: `["dbfs:/path/to/config.csv"]`
* Args(1): Number of threads to complete the task in parallel. Default == 4. (Optional)
  * EX: `["dbfs:/path/to/config.csv", "4"]`
* Args(2): Pipelines to be executed. Default == "Bronze,Silver,Gold"
  * If you wanted to split Bronze into one task and Silver/Gold into another task the arguments would look like 
    the examples below.
    * Bronze Only Task - `["dbfs:/path/to/config.csv", "4", "Bronze"]`
    * Silver/Gold Task - `["dbfs:/path/to/config.csv", "4", "Silver,Gold"]`
  * Running all the pipelines together will maximize cluster utilization but there are often reasons to split the 
    pipelines thus we've added support.
         

![newUIJarSetup](/images/GettingStarted/mswjob.png)