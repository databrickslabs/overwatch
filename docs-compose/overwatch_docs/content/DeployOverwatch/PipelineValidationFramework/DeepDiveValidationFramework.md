---
title: "Deep Dive Validation Framework"
date: 2024-03-16T18:06:39+05:30
---

This pipeline validation module will run after overwatch deployment is done.For now,this module will be used by the support team for checking the data quality of recent Deployments.

{{% notice note %}}
Upon execution, the Validation Framework is designed to process only those records pending validation, thereby 
ensuring that previously validated records are not subjected to redundant validation checks. For instance, 
following a fresh deployment, executing the Validation Framework will initiate validation exclusively for 
records associated with the recent deployment. Subsequently, if the framework is executed after the second and third 
deployments, it will selectively validate records related to these deployments that have not yet been validated, 
thus maintaining the efficiency and relevance of the validation process.
{{% /notice %}}

## High Level Design for Validation Framework
Here is the high level design of the framework
 ![PipelineValidationFramework](/images/DeployOverwatch/Pipeline_Validation.png)

1. Overwatch Deployment is finished as per the usual process.
2. Trigger the Validation Framework and aS per the rules defined in Validation Framework the data in tables would be validated.
3. This decision point checks the results of the pipeline validation.
4. After validation framework is executed, a health check report for the pipeline is generated. This report likely
   contains details on the validation checks and indicates that the pipeline is healthy and functioning as expected. 
5. If any validation check fails, the snapshot of rows that got failed are moved to a quarantine zone. This would be used for 
   debugging purpose.

## Validation Framework Reports
Upon running validation framework you would get 2 types of records.
1. HeathCheck_Report.
2. Quarantine_Report.

### HeathCheck_Report
For every run the snapshot of the run for every table for each rule would be updated to pipeline healthcheck report.
Structure for the HealthCheck Report is
| Column Name             | Type   | Description                                                                                      | Sample Value                                                              |
|-------------------------|--------|--------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| healthcheck_id          | uuid   | Unique ID for Healthcheck Report                                                                 | 272b83eb-1362-456a-8dae-26c3083181ac                                      |
| etl_database            | string | Overwatch etl database name                                                                      | ow_etl_azure_0721                                                         |
| table_name              | string | Overwatch Table Name                                                                             | clusterstatefact_gold                                                     |
| healthcheck_rule        | string | Denote the rule which would be run on the table data                                             | Driver_Node_Type_ID Should Not be NULL                                    |
| rule_type               | string | Define the type of the validation rule. Types: `Pipeline_Report_Validation`, `Single_Table_Validation`, `Cross_Table_Validation` | Pipeline_Report_Validation                |
| healthcheckMsg          | string | Healthcheck status for the table data for the specific healthcheck_rule                          | HealthCheck Failed: got 63 driver_node_type_ids which are null            |
| overwatch_runID         | string | Overwatch Run_ID for which Snapshot is being taken                                               | b8985023d6ae40fa88bb6daef7f46725                                          |
| snap_ts                 | long   | Snapshot Timestamp in yyyy-mm-dd hh:mm:ss format                                                 | 2023-09-27 07:43:39                                                       |
| quarantine_id           | uuid   | Unique ID for Quarantine Report                                                                  | cd6d7ecc-b72d-4c16-b8ca-01a6df4fba9c                                      |

### Quarantine_Report
Rows which would fail as per the rules configured in validation framework would be updated to quarantine report
| Column Name             | Type         | Description                                                                                                         | Sample Value                                                                                       |
|-------------------------|--------------|---------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| quarantine_id           | uuid         | Unique ID for Quarantine Report                                                                                     | cd6d7ecc-b72d-4c16-b8ca-01a6df4fba9c                                                               |
| etl_database            | string       | Overwatch etl database name                                                                                         | ow_etl_azure_0721                                                                                  |
| table_name              | string       | Overwatch Table Name                                                                                                | clusterstatefact_gold                                                                              |
| healthcheck_rule_failed | string       | Denote the rule for which this specific row has been quarantined                                                    | Driver_Node_Type_ID Should Not be NULL                                                             |
| rule_type               | string       | Define the type of the validation rule. Types: `Pipeline_Report_Validation`, `Single_Table_Validation`, `Cross_Table_Validation` | Pipeline_Report_Validation                                                            |
| healthcheck_type        | string       | Denote the failure type of the health check rule. This can be either `Warning` or `Failure`                         | Failure                                                                                            |
| keys                    | string(json) | Primary key for the Overwatch table                                                                                 |{"cluster_id":"0831-200003-6bhv8y0u","state":"TERMINATING","unixTimeMS_state_start":1693514245329}  |
| snap_ts                 | long         | Snapshot Timestamp in yyyy-mm-dd hh:mm:ss format                                                                    | 2023-09-27 07:43:39                                                                                |

After the execution of validation framework would be completed both the above-mentioned report would be 
created as delta file in dbfs location. You can get the information of the delta path in the stacktrace
of the validation framework run like below
![Report StackTrace](/images/DeployOverwatch/Validation_Trace.png)


## Rule_Type in Validation Framework

As mentioned above in HeathCheck_Report structure currently,there are 3 rule_type available in validation framework. They are

1. Pipeline_Report_Validation
2. Single_Table_Validation
3. Cross_Table_Validation

By Default when we execute validation_Framework healthcheck_rules of all these 3 rule_types would be executed. 
But through input arguments we can configure our framework run. More on this would be discussed in later section.

Let's see how in detail how this rule types are different from each other.

### Pipeline_Report_Validation
This rule type is only for Pipeline_Report Validation. In this rule type we basically have 2 healthCheck_rule.

* **Check If Any Module is EMPTY**
* **Check If Any Module is FAILED**


### Single_Table_Validation
The healthCheck_rules with this rule_type would be applied to single table. Basically it will check data quality of single tables.
Currently, below tables are in scope of this rule type:

* clusterstatefact_gold
* jobruncostpotentialfact_gold
* cluster_gold
* sparkjob_gold
* sql_query_history_gold
* jobrun_gold
* job_gold

Different healthCheck_rules would be applied on each of these tables.
#### clusterstatefact_gold
* Cluster_ID_Should_Not_be_NULL
* Driver_Node_Type_ID_Should_Not_be_NULL
* Node_Type_ID_Should_Not_be_NULL_for_Multi_Node_Cluster
* DBU_Rate_Should_Be_Greater_Than_Zero_for_Runtime_Engine_is_Standard_Or_Photon
* Total_Cost_Should_Be_Greater_Than_Zero_for_Databricks_Billable
* Check_Whether_Any_Single_Cluster_State_is_Running_For_Multiple_Days


#### jobruncostpotentialfact_gold
* Job_ID_Should_Not_be_NULL
* Driver_Node_Type_ID_Should_Not_be_NULL
* Job_Run_Cluster_Util_value_Should_Not_Be_More_Than_One
* Check_Whether_Any_Job_is_Running_For_Multiple_Days

#### cluster_gold
* Cluster_ID_Should_Not_be_NULL
* Driver_Node_Type_ID_Should_Not_be_NULL
* Node_Type_ID_Should_Not_be_NULL_for_Multi_Node_Cluster
* Cluster_Type_Should_be_In_Between_Serverless_SQL-Analytics_Single-Node_Standard_High-Concurrency
* 

#### sparkjob_gold
* Cluster_ID_Should_Not_be_NULL
* Job_ID_Should_Not_be_NULL
* db_id_in_job_Should_Not_be_NULL_When_db_Job_Id_is_Not_NULL

#### sql_query_history_gold
* Warehouse_ID_Should_Not_be_NULL
* Query_ID_Should_Not_be_NULL

#### jobrun_gold
* Job_ID_Should_Not_be_NULL
* Run_ID_Should_Not_be_NULL
* Job_Run_ID_Should_Not_be_NULL
* Task_Run_ID_Should_Not_be_NULL
* Cluster_ID_Should_Not_be_NULL

#### job_gold
* Job_ID_Should_Not_be_NULL
* Action_Should_be_In_Between_snapimpute_create_reset_update_delete_resetJobAcl_changeJobAcl

### Cross_Table_Validation
The healthCheck_rules with this rule_type would be applied to 2 or more tables. This rule type is basically used to 
validated data consistency across tables.

HealthCheck_rules of this rule_type are as below:
* JOB_ID_Present_In_jobRun_gold_But_Not_In_jobRunCostPotentialFact_gold
* JOB_ID_Present_In_jobRunCostPotentialFact_gold_But_Not_In_jobRun_gold
* CLUSTER_ID_Present_In_jobRun_gold_But_Not_In_jobRunCostPotentialFact_gold
* CLUSTER_ID_Present_In_jobRunCostPotentialFact_gold_But_Not_In_jobRun_gold
* NOTEBOOK_ID_Present_In_notebook_gold_But_Not_In_notebookCommands_gold
* NOTEBOOK_ID_Present_In_notebookCommands_gold_But_Not_In_notebook_gold
* CLUSTER_ID_Present_In_clusterStateFact_gold_But_Not_In_jobRunCostPotentialFact_gold
* CLUSTER_ID_Present_In_jobRunCostPotentialFact_gold_But_Not_In_clusterStateFact_gold
* CLUSTER_ID_Present_In_cluster_gold_But_Not_In_clusterStateFact_gold
* CLUSTER_ID_Present_In_clusterStateFact_gold_But_Not_In_cluster_gold


## How to Run Validation Framework
Currently, we can execute validation framework through notebook only. Depend on requirement running through databricks workflow
would be added in the future.

Execution would be started upon calling `PipelineValidation()` function. Below are the input arguments of this function:
| Param                | Type    | Optional | Default Value | Description                                                                 |
|----------------------|---------|----------|---------------|-----------------------------------------------------------------------------|
| etlDB                | String  | No       | NA            | Overwatch etl database name on which Validation Framework need to be run    |
| allRun               | Boolean | Yes      | Yes           | Boolean flag act on and off switch for validation of all overwatch_runID in pipeline_report. By Default all overwatchRun_IDs are validated.|
| tableArray           | String  | Yes      | Array()       | Array of tables on which Single Table Validation need to be performed. If it is empty then all the tables mentioned in single table validation would be in scope.|
| crossTableValidation | Boolean | Yes      | Yes           | Boolean flag act as on and off switch for cross table validation. By default, cross table validation is active.            |

Here are the screenshots of Validation Framework run with default setting and with custom configuration using input arguments:

### Default Run
![Default Run](/images/DeployOverwatch/Default_Validation_Run.png)
Here we can see from the above screenshot that:
* By Default Pipeline_Report_Validation would be performed.
* Cross Table Validation would be performed
* Single table validation would be performed for all the tables as mentioned above.

### All_run is False
![All Run](/images/DeployOverwatch/allRun.png)
* By Default Pipeline_Report_Validation would be performed.If allRun is false then only the recent runs which are yet to be validated would be validated.
* Cross Table Validation would be performed
* Single table validation would be perfor                                                                                                         +;;; vv;med for all the tables as mentioned above.

### Validation Framework run with specific tables mentioned in tableArray
![Table_Array](/images/DeployOverwatch/Table_Array.png)
Here we can see from the above screenshot that:
* By Default Pipeline_Report_Validation would be performed.
* Cross Table Validation would be performed
* Single table validation is performed on clusterstatefact_gold,jobruncostpotentialfact_gold as mentioned in `tableArray` param.

### Validation Framework run with specific tables mentioned in tableArray and Disable Cross Validation
![cross_validation](/images/DeployOverwatch/cross_validation.png)
Here we can see from the above screenshot that:
* By Default Pipeline_Report_Validation would be performed.
* Single table validation is performed on clusterstatefact_gold,jobruncostpotentialfact_gold as mentioned in `tableArray` param.
* Cross Table Validation would not be performed as `crossTableValidation` flag us `False` here.

