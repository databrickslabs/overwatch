---
title: "System Table Configuration"
date: 2024-01-23T16:30:21+05:30
---

## Configuration changes required for System Table Integration

There is no unique configuration process for System Table. The only change required to the configuration for integration
is below - 

* **auditlogprefix_source_path** - Instead of adding a fully qualified path (s3 or GC) for auditlog, 
add keyword **system** in this column. This will enable the system table integration.

**For migrating to system tables above same congifuration changes can be used.**

For all other configurations, please follow the [Configuration]({{%relref "DeployOverwatch/ConfigureOverwatch/Configuration.md"%}})

{{% notice note %}}
Once the customer migrate to system table (version 0.8.0.0), they cannot revert back to legacy Deployment.
{{% /notice %}}


{{% notice note %}}
For Azure deployments since Eventhub is not required for System Table Integration, we can leave these 
fields blank - eh_name, Event hub name, eh_name, eh_conn_string, aad_tenant_id, aad_client_id, aad_client_secret_key, 
aad_authority_endpoint
{{% /notice %}}

## Configuration changes required for Multi Account System Table Integration
The process for achieving multi-account deployment is straightforward. In the configuration table, for each workspace 
in a different account, you'll need to enter a value for the sql_endpoint column, a new warehouse needs to be created 
or you can use an already existing warehouse. We encourage you to use a serverless warehouse for this purpose. 
The warehouse endpoint details are then passed to Overwatch through the configuration, specifically in the sql_endpoint 
column. **sql_endpoint column needs to be added only for multi account deployment.** 
Below are the details

* **sql_endpoint** - the http path from the sql_warehouse need to be added in this field. This http path can be found 
in the connection details table of the warehouse.
* **auditlogprefix_source_path** - Instead of adding a fully qualified path (s3 or GC) for auditlog,
add keyword **system** in this column. This will enable the system table integration.

For all other configurations, please follow the
[Configuration]({{%relref "DeployOverwatch/ConfigureOverwatch/Configuration.md"%}})