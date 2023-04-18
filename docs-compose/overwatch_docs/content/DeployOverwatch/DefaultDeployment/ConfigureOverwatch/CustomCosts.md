---
title: "Custom Costs"
date: 2022-12-13T14:35:00-05:00
weight: 2
---

## Fine-Tuning Your Costs
Every customer has their own contracts and this means that the costs associated with cloud compute and DBUs may differ 
between customers. To ensure the costs in Overwatch are as accurate as possible it's important that these costs are 
configured as accurately as possible.

### Configuring Custom Costs
There are three essential components to the cost function:
* The node type (instanceDetails.Api_Name) and its associated contract price (instanceDetails.Compute_Contract_Price) 
  by Workspace
* The node type (instanceDetails.Api_Name) and its associated DBUs per hour (instanceDetails.Hourly_DBUs). 
  These should be accurate from the default load but Databricks may adjust their DBUs/Hour by node type. This is 
  especially true when a node goes from beta to GA.
* The DBU contract prices for the SKU under which your DBUs are charged such as:
    * Interactive
    * Automated
    * DatabricksSQL
        * Databricks currently has 3 SKUs (classic/pro/serverless) but Overwatch is not able to accurately report
          on DBSQL pricing at this time due to data not available in the customer-facing Databricks Product. 
          When this data becomes available, Overwatch will integrate it and enable DBSQL cost tracking. In the 
          meantime Overwatch will does it's best to estimate DBSQL pricing so for this SKU just put your average 
          $DBU cost or a close estimate to your sku price here.
    * JobsLight

The DBU contract costs are captured from the
[Overwatch Configuration]({{%relref "DeployOverwatch/ConfigureOverwatch/Configuration.md"%}}/#databrickscontractprices) maintained
as a slow-changing-dimension in the [dbuCostDetails table]({{%relref "dataengineer/definitions/_index.md"%}}/#dbucostdetails).
The compute costs and dbu to node
associations are maintained as a slow-changing-dimension in the
[instanceDetails]({{%relref "dataengineer/definitions/_index.md"%}}/#instancedetails) table.
* **IMPORTANT** These tables are automatically created in the dataTarget upon first initialization of the pipeline.
* **DO NOT** try to manually create the target database outside of Overwatch as that will lead to database validation errors.
* **INSTANCE DETAILS IS NOT MAINTAINED BY THE OVERWATCH PIPELINE** After the first run, this table will not be edited 
  by the Overwatch Pipeline again. Customers often update the compute_contract_price to reflect their contract prices 
  with the cloud vendor. Updating this table would override these customizations. If you are using instance types 
  that are not present in instanceDetails table you will see $0 costs. Be sure to periodically add new node types.

**To customize costs** configure and complete your first Bronze run. Before running Silver/Gold, alter the 
  instanceDetails table to your satisfaction. Note that this is only true for instanceDetails not dbuCostDetails as 
  Overwatch will maintain this table for you.

Note that each subsequent workspace referencing the same dataTarget will append default compute prices to instanceDetails
table if no data is present for that organization_id (i.e. workspace).
If you would like to customize compute costs for all workspaces,   
export the instanceDetails dataset to external editor (after first init), add the required metrics referenced above
for each workspace, and update the target table with the customized cost information. Note that the instanceDetails
object in the consumer database is just a view so you must edit/overwrite the underlying table in the ETL database. The view
will automatically be recreated upon first pipeline run.

**IMPORTANT** These cost lookup tables are slow-changing-dimensions and thus they have specific rule requirements; 
familiarize yourself with the details at the links below. If the rules fail, the Gold Pipeline will fail with specific 
costing errors to help you resolve it.
* [InstanceDetails Table Details]({{%relref "dataengineer/definitions/_index.md"%}}/#instancedetails)
* [dbuCostDetails Table Details]({{%relref "dataengineer/definitions/_index.md"%}}/#dbucostdetails)

[Helpful Tool (AZURE_Only)](https://azureprice.net/) to get pricing by region by node.

Sample compute details available below. These are only meant for reference, they do not have all required fields.
Follow the instruction above for how to implement custom costs.
{{< rawhtml >}}
<a href="https://drive.google.com/file/d/1tj0GV-vX1Ka9cRcJpJSwkQx6bbpueSwl/view?usp=sharing" target="_blank">AWS Example</a>
{{< /rawhtml >}} |
{{< rawhtml >}}
<a href="https://drive.google.com/file/d/13hYZrOAmzLwIjfgNz0YWx-qE2TdWe0-c/view?usp=sharing" target="_blank">Azure Example</a>
{{< /rawhtml >}}

## Compute Costs Are Estimated
Overwatch is not able to determine the VM SKU provided by your cloud provider (i.e. OnDemand/Spot/Reserved); thus the 
exact price of compute asset at the time of provisioning isn't available. The compute_contract_price configured in
[instanceDetails]({{%relref "dataengineer/definitions/_index.md"%}}/#instancedetails) is best configured as the 
average price you received over some previous period to allow Overwatch to best estimate the compute costs; 
nonetheless, the compute costs are just that, estimates.