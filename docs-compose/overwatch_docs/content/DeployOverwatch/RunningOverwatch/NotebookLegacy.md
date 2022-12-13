---
title: "As A Notebook (Legacy)"
date: 2022-12-12T12:04:43-05:00
weight: 4
---

{{% notice warning%}}
As of version 0.7.1.0 Overwatch will begin sunsetting this deployment method. Please reference 
[Running A Job As A Notebook]({{%relref "DeployOverwatch/RunningOverwatch/Notebook.md"%}}) for the updated method for 
deploying Overwatch. This method will likely be deprecated in Overwatch version 0.8 and no longer be supported in 0.9. 
{{% /notice %}}

The new deployment method provides support for a "single-workspace deployment" or "multi-workspace deployment" where 
a single Overwatch job is configured and loads data from all workspaces. More details available in
[Running A Job As A Notebook]({{%relref "DeployOverwatch/RunningOverwatch/Notebook.md"%}}).

## Legacy Deployment Method
The legacy deployment method requires an Overwatch job to run locally on each workspace that is to be monitored. 

### The Process
* Use a "Runner Notebook" to build a JSON config string
* Use the JSON config string to Initialize Overwatch Workspace
* Use the Overwatch Workspace Object to launch the Pipelines

Below is a pseudocode example. This is essentially all that is happening. All the commands and widgets in the 
runner notebooks are just there to help customers derive the json config string.
```scala
val myJsonConfig = """someJSONString"""
val workspace = Initializer(myJsonConfig)
Bronze(workspace).run()
Silver(workspace).run()
Gold(workspace).run()
```

{{% notice note%}}
Getting a strong first run is critical. Please review 
[Getting A Strong First Run]({{%relref "DataEngineer/AdvancedTopics"%}}/getting-a-strong-first-run) in 
advanced topics before continuing if this is your first deployment.
{{% /notice %}}

### Jump Start Notebooks
The following notebooks will demonstrate a typical setup for Overwatch deployments.
Populate the necessary variables/widgets for your environment.
* AZURE ([HTML 0.7.0+](/assets/GettingStarted/azure_runner_docs_example_070.html) / [DBC 0.7.0+](/assets/GettingStarted/azure_runner_docs_example_070.dbc))
* AWS ([HTML 0.7.0+](/assets/GettingStarted/aws_runner_docs_example_070.html) / [DBC 0.7.0+](/assets/GettingStarted/aws_runner_docs_example_070.dbc))
* AZURE ([HTML 0.6.0+](/assets/GettingStarted/azure_runner_docs_example_060.html) / [DBC 0.6.0+](/assets/GettingStarted/azure_runner_docs_example_060.dbc))
* AWS ([HTML 0.6.0+](/assets/GettingStarted/aws_runner_docs_example_060.html) / [DBC 0.6.0+](/assets/GettingStarted/aws_runner_docs_example_060.dbc))
* AZURE ([HTML 0.5.0.4+](/assets/GettingStarted/azure_runner_docs_example_0504.html) / [DBC 0.5.0.4+](/assets/GettingStarted/azure_runner_docs_example_0504.dbc))
* AWS ([HTML 0.5.x](/assets/GettingStarted/aws_runner_docs_example_042.html) / [DBC 0.5.x](/assets/GettingStarted/aws_runner_docs_example_042.dbc))
