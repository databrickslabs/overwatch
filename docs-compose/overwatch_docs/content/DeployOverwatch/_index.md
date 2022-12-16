---
title: "Deploy Overwatch"
date: 2022-12-12T11:28:39-05:00
weight: 2
---

{{% notice warning%}}
**EXISTING CUSTOMERS COMING FROM VERSION < 0.7.1.0** As of version 0.7.1.0 Overwatch will begin sunsetting
the "legacy" deployment method and
[configuration]({{%relref "DeployOverwatch/ConfigureOverwatch/ConfigurationLegacy.md"%}}). Please review the
benefits of using the new deployment method and plan to switch to this new deployment method by end of Q3 2023.
**New customers may disregard this notice.**
{{% /notice %}}

Overwatch is a pipeline that executes to aggregate and normalize all of the logs from all the supported sources and 
make them easy to interrogate for insights. The steps to deploying Overwatch are pretty simple but there are some 
specific details that may pertain to your deployment. Below are the top level steps that this section will walk you 
through the process.

## Steps
1. Configure Cloud Infrastructure
2. Build / Apply Security
3. Configure Overwatch
4. Run and Schedule the Job\[s\]
5. Productionize Your Deployment
6. Share The Data

### Configuring Your Cloud
Select your cloud to find the details for cloud configuration here. 
([AWS]({{%relref "DeployOverwatch/CloudInfra/AWS.md"%}}) | [Azure]({{%relref "DeployOverwatch/CloudInfra/Azure.md"%}}))

### Build / Apply Security
Reference [Security Considerations]({{%relref "DeployOverwatch/ConfigureOverwatch/SecurityConsiderations.md"%}}) page 
to help you build and apply a security model commensurate with your needs.

### Configure Overwatch
Reference the [configuration page]({{%relref "DeployOverwatch/ConfigureOverwatch/Configuration.md"%}}) to clarify the 
configuration details and help you get started.

For legacy deployment methods please reference the 
[Legacy Configuration Page]({{%relref "DeployOverwatch/ConfigureOverwatch/ConfigurationLegacy.md"%}})

### Run and Schedule the Job\[s\]
Decide whether you'd like to execute Overwatch as a [JAR]({{%relref "DeployOverwatch/RunningOverwatch/JAR.md"%}}) 
or a [NOTEBOOK]({{%relref "DeployOverwatch/RunningOverwatch/Notebook.md"%}}) and schedule a job to periodically 
execute the job.

It's recommended to run Overwatch as a JAR as it unifies the deployment and doesn't depend on another asset 
(the notebook) to secure and ensure no one edits, moves, or deletes it.

### Productionize Your Deployment
Now that you're live it's important to 
[optimize everything]({{%relref "DataEngineer/AdvancedTopics"%}}/#optimizing-overwatch) and ensure the data is safe 
in case something unexpected happens -- see [Productionizing]({{%relref "DataEngineer/Productionizing.md"%}})

### Share The Data
Now you're ready to onboard consumers across your workspaces. Details about how to do that can be found in the 
[Sharing Overwatch]({{%relref "DeployOverwatch/SharingOverwatch.md"%}}) page.