---
title: "System Table Integration"
date: 2024-01-22T19:35:23+05:30
---

{{% notice note %}}
At this time Overwatch only supports System Table Integration with AWS and Azure. 
Currently System Table Integration is not supported on GCP.
{{% /notice %}}

Databricks system tables offer a robust mechanism for managing and understanding the intricacies of a Databricks 
environment. From metadata exploration to performance optimization and security auditing, these tables provide a 
comprehensive foundation for users and administrators to derive valuable insights and make informed decisions in a 
collaborative and data-driven environment. Follow these documents to know more about System Tables 
[Databricks Docs for System Tables](https://docs.databricks.com/en/administration-guide/system-tables/index.html)

In our continuous efforts to enhance the efficiency and robustness of Overwatch, we have transitioned to a 
system table integration for audit logs. This strategic move brings forth numerous advantages, further optimizing the 
monitoring and logging capabilities of the tool. Currently, our integration focuses on leveraging system table 
audit logs, but in future releases, we will be expanding our integration to include other system tables as well. Follow 
these documents to know more about 
[System Table Audit Logs](https://docs.databricks.com/en/administration-guide/system-tables/audit-logs.html)

Advantages of Moving to System Table Integrations: 

* **Seamless Integration with Overwatch**:
System table integration seamlessly aligns with Overwatch.
The synergy between system tables and Overwatch ensures a harmonious integration, providing a unified platform for 
comprehensive monitoring and analysis.

* **Simplified Setup within Overwatch**:
Integration with system tables leads to a simplified setup within Overwatch. The need to configure audit log settings 
independently within the Overwatch configurations is eliminated. This simplicity ensures a more user-friendly experience
for both setup and ongoing management. 

* **Reduced Dependency on IAM Roles and Service Accounts**:
The move to system table integrations diminishes the necessity for setting up extra IAM roles or service accounts 
specifically for audit log management within Overwatch. This reduction in dependencies streamlines the integration 
process, enhancing the overall efficiency of the tool.

* **Effortless Migration**:
The migration process to system tables is designed to be effortless within the Overwatch framework. 
The transition is smooth, minimizing disruptions and ensuring continuous monitoring capabilities 
without requiring extensive reconfigurations.

* **Extended Data Retention for Azure Deployments in Overwatch**:
Particularly advantageous for Overwatch Azure deployments, the use of system tables extends the 
audit log retention period beyond 30 days for audit log bronze. This eliminates the previous dependency on Events Hub's
retention period.

## System Table Integration Configuration Specific Differences
As mentioned earlier, the deployment process is almost the same as other deployments. Therefore, the
[Deployment Guide]({{%relref "DeployOverwatch"%}}) contains relevant information that will guide your deployment. 
Specific deviations are referenced below and detailed in this section of the documentation.

* [System Table Integration Pre-Requisites]({{%relref "DeployOverwatch/SystemTableIntegration/SystemTablePreReq"%}})
* [Configuration Details]({{%relref "DeployOverwatch/SystemTableIntegration/SystemTableConfiguration"%}})

## Migrating to System Table

{{% notice warning %}}
Once the deployment is migrated to System Tables, it cannot be reverted back to source the audit logs from S, GC Bucket
or Azure Event Hubs.
{{% /notice %}}
The migration to system tables is straightforward. Organizations can effortlessly transition from existing 
configurations to system table integrations, minimizing downtime and ensuring a smooth migration experience.
Follow the below steps to migrate to System Table Integration: 
[Configuration Details]({{%relref "DeployOverwatch/SystemTableIntegration/SystemTableConfiguration"%}})

## Fetching data from Multi Account System Table  

With the integration of System Table in Overwatch, fetching system table data from multiple different accounts 
becomes easy, enabling seamless multi-account deployments. This setup requires minimal adjustments in 
configuration and additional infrastructure setup in the workspaces of other accounts.
Follow the link to configure 
[Multi Account System Table Integration]({{%relref "DeployOverwatch/SystemTableIntegration/SystemTableConfiguration"%}}/#ConfigurationChangesRequiredForMultiAccountSystemTableIntegration)