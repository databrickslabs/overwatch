---
title: "As A Notebook"
date: 2022-12-12T12:04:26-05:00
weight: 2
---
## Deploying Overwatch As A Notebook
Notebooks can either be run manually or scheduled to run as a job. While the notebook can be scheduled as a job, 
it's strongly recommended that Overwatch be run as a JAR instead of a notebook. Notebook execution is great for 
rapid testing and validation.

**This deployment method requires Overwatch Version 0.7.1.0+**

### Converting Your Config From CSV To Delta
**AS OF version 0.7.1.1** you may now use a **CSV** OR a **Delta Table** OR **Delta Path** for your config

We heard you! Customers want to set up their initial config as a CSV to get all their workspaces configured but once 
they're configured it's challenging to make small edits. Now you can convert your initial CSV to delta and instead of 
referencing the path to a config.csv file instead reference a delta table or delta path to the location of your 
config. This allows for simple update statements to switch records from *active == true* to *active == false* or 
quickly disable scopes, etc. Below are the details of how to reference the relevant path and some code to quickly and 
safely convert your CSV to a delta source. To do so just upgrade to 0.7.1.1+ and everywhere the docs reference the 
config.csv switch it to reference the appropriate path.

| Source Config Format | Path Reference                                 |
|----------------------|------------------------------------------------|
| CSV                  | dbfs:/myPath/overwatch/configs/prod_config.csv |
| Delta Table          | database.overwatch_prod_config                 |
| Delta Path           | dbfs:/myPath/overwatch/configs/prod_config     |

**Convert To Delta Table** (Example)
```scala
spark.read
  .option("header", "true")
  .option("ignoreLeadingWhiteSpace", true)
  .option("ignoreTrailingWhiteSpace", true)
  .csv("/path/to/config.csv")
  .coalesce(1)
  .write.format("delta")
  .saveAsTable("database.overwatch_prod_config")
```

**Convert To Delta Path** (Example)
```scala
spark.read
  .option("header", "true")
  .option("ignoreLeadingWhiteSpace", true)
  .option("ignoreTrailingWhiteSpace", true)
  .csv("/path/to/config.csv")
  .coalesce(1)
  .write.format("delta")
  .save("/myPath/overwatch/configs/prod_config")
```


### Jumpstart Notebook
Below is an example deployment. When you're ready to get started simply download the rapid start linked notebook below.

* **071.x** ( [**HTML**](/assets/DeployOverwatch/071x_Runner.html) | [**DBC**](/assets/DeployOverwatch/071x_Runner.dbc) )

### Deployment Example
```scala
import com.databricks.labs.overwatch.MultiWorkspaceDeployment
val configCsvPath = "dbfs:/FileStore/overwatch/workspaceConfig.csv" // Path to the config.csv

// temp location which will be used as a temp storage. It will be automatically cleaned after each run.
val tempLocation = "/tmp/overwatch/templocation"

// number of workspaces to validate in parallel. Exceeding 20 may require larger drivers or additional cluster config considerations
// If total workspaces <= 20 recommend setting parallelism == to workspace count 
val parallelism = 4

// Run validation
MultiWorkspaceDeployment(configCsvPath, tempLocation)
  .deploy(parallelism)

// To run only specific pipelines (i.e. Bronze / Silver Gold) as second argument can be passed to deploy as a 
// comma-delimited list of pipelines to run
// ex: MultiWorkspaceDeployment(configCsvPath, tempLocation).deploy(parallelism,"Bronze") 
//     MultiWorkspaceDeployment(configCsvPath, tempLocation).deploy(parallelism,"Silver")
//     MultiWorkspaceDeployment(configCsvPath, tempLocation).deploy(parallelism,"Gold")
//     MultiWorkspaceDeployment(configCsvPath, tempLocation).deploy(parallelism,"Bronze,Silver")
//     MultiWorkspaceDeployment(configCsvPath, tempLocation).deploy(parallelism,"Silver,Gold")
```

### Review The Deployment Status
Once the deployment is completed, all the deployment update details will be stored in a deployment report. It will 
be generated in <etl_storage_prefix>/report/deploymentReport as delta table.
Run the below query to check the deployment report.

**NOTE** The deployment report maintains a full history of deployments. If you've deployed multiple times be sure
to look at the snapTS and only review the validations relevant for the latest deployment.
```sql
select * from delta.`<etl_storage_prefix>/report/deploymentReport`
order by snapTS desc
```

```scala
display(
  spark.read.format("delta").load("<etl_storage_prefix>/report/deploymentReport")
    .orderBy('snapTS.desc)
)
```
