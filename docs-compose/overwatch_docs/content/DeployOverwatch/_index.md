---
title: "Deploy Overwatch"
date: 2022-12-12T11:28:39-05:00
weight: 2
---

{{% notice warning%}}
**EXISTING CUSTOMERS COMING FROM VERSION < 0.7.1.0** As of version 0.7.1.0 Overwatch will begin sunsetting
the "legacy" deployment ([Notebook]({{%relref "DeployOverwatch/RunningOverwatch/NotebookLegacy.md"%}}) | 
[JAR]({{%relref "DeployOverwatch/RunningOverwatch/JARLegacy.md"%}})) method and
[Legacy Configuration]({{%relref "DeployOverwatch/ConfigureOverwatch/ConfigurationLegacy.md"%}}). Please review the
benefits of using the new deployment method and plan to switch to this new deployment method by end of Q3 2023.
**New customers may disregard this notice.**
{{% /notice %}}

Overwatch is a pipeline that executes to aggregate and normalize all of the logs from all the supported sources and 
make them easy to interrogate for insights. The steps to deploying Overwatch are pretty simple but there are some 
specific details that may pertain to your deployment. Below are the top level steps that this section will walk you 
through the process.

When wanting to monitor your Workspaces, you might want to:
1. [Deploy Overwatch for the first time](#first-overwatch-deployment)
2. [Add a new workspace to an existing Overwatch deployment](#add-workspace-to-existing-overwatch-deployment)

## First Overwatch deployment
### Steps
**1. Configure Cloud Infrastructure** 
   
   Select your cloud to find the details for cloud configuration here.
   ([AWS]({{%relref "DeployOverwatch/CloudInfra/AWS.md"%}}) | [Azure]({{%relref "DeployOverwatch/CloudInfra/Azure.md"%}}) |
   [GCP]({{%relref "DeployOverwatch/CloudInfra/GCP.md"%}}))

**2. Build / Apply Security**
   
   Reference [Security Considerations]({{%relref "DeployOverwatch/ConfigureOverwatch/SecurityConsiderations.md"%}}) page
   to help you build and apply a security model commensurate with your needs.

**3. Configure Overwatch inputs**
   
   Reference the [configuration page]({{%relref "DeployOverwatch/ConfigureOverwatch/Configuration.md"%}}) to clarify the
   configuration details and help you get started.

**4. Run and Schedule the Job\[s\]**
   
   Decide whether you'd like to execute Overwatch as a [JAR]({{%relref "RunningOverwatch/JAR.md"%}})
   or a [NOTEBOOK]({{%relref "RunningOverwatch/Notebook.md"%}}) and schedule a job to periodically
   execute the job. 
   It's recommended to run Overwatch as a JAR as it unifies the deployment and doesn't depend on another asset 
   (the notebook) to secure and ensure no one edits, moves, or deletes it.

**5. Productionize Your Deployment**

   Now that you're live it's important to
   [optimize everything]({{%relref "DataEngineer/AdvancedTopics"%}}/#optimizing-overwatch) and ensure the data is safe
   in case something unexpected happens -- see [Productionizing]({{%relref "DataEngineer/Productionizing.md"%}})

**6. Share/Utilize Overwatch**
   
   Now you're ready to onboard consumers across your workspaces. Details about how to do that can be found in the
   [Sharing Overwatch]({{%relref "DeployOverwatch/SharingOverwatch.md"%}}) page.

## Add workspace to existing Overwatch deployment
When adding a new workspace, all you have to do is add a new row in the configuration table for Overwatch containing the 
new workspace's information. Please go through the previous steps for cloud infrastructure and security considerations
to keep in mind to ensure the driver workspace can monitor the new workspace

## Multi-Workspace Monitoring - Considerations
As of 0.7.1.0 Overwatch is able to monitor remote workspaces but having one and only one global deployment often 
doesn't meet customer needs and there are some requirements for a deployment to monitor remote workspaces. More details 
are available on the [Security Considerations]({{%relref "DeployOverwatch/ConfigureOverwatch/SecurityConsiderations.md"%}}) and 
[Validations]({{%relref "DeployOverwatch/ConfigureOverwatch/Validation.md"%}}) pages. The most important things to consider are: 
* Access to remote workspaces via API Token -- a token (PAT) authorizing access to the remote workspace is required to 
be provisioned.
* The Overwatch cluster must have direct read access (not via a mount point) to all cluster logging cloud storage
* The remote workspace may not have more than 50 mount points

If some of your workspaces do not meet these requirements; that's ok, a local deployment with a simpler config will 
need to be created for those outliers. It's completely fine to have a primary deployment that manages 20 workspaces and 
a few workspaces that have to have their own Overwatch Job. The data from each deployment can be dropped into the same 
or multiple targets, this is all part of the configuration.

## Overwatch Landscapes
### An Example Scenario
Assume you have 23 workspaces, 20 of which meet the criteria discussed above and 3 that need to be locally configured. 
All 23 workspaces an output their data into the same database (or multiple) but this would require that Overwatch run on 
4 workspaces, 1 to manage the 20 and 1 on each of the 3 with special requirements.

### Production
Production can hold the data for all 23 workspaces even if some of the workspaces are deemed "non-prod". This is ideal 
to allow an analyst to identify efficiency gains and metrics globally from a single location.

### Non-Production
A non-prod Overwatch deployment is also recommended so that when new versions come out and/or schema upgrades come out 
your team can review and test the upgrade and update any dependent dashboards before you upgrade all 23 workspaces. 

A non-prod Overwatch deployment typically consists of a subset of the 23 workspaces (usually 3-5 workspaces). 
This deployment will always be upgraded and validated before the global production deployment. This pattern enables 
Change Management best practices. Non-Prod and Prod can even be deployed on the same workspace just with two different 
configurations such that the non-prod has its own database names and storage locations.