---
title: "Logical Flow and Reports"
---

Overwatch provides a special pipeline validation module to perform a
number of data quality checks on the output it produces in its target
tables.  Pipeline validation may run as frequently as the regular
Overwatch ETL pipeline/module runs, i.e. after each pipeline
"deployment".  This page shows how to add this pipeline validation
step to an Overwatch workflow and describes its functionality in
detail.

{{% notice note %}}

The pipeline validation module is designed to perform its checks on
records that it has not already validated in a previous run to avoid
redundant validation checks. Given that validation has completed
normally following a previous Overwatch pipeline deployment (i.e. ETL
run), only new records produced in one or more subsequent deployments
are subject to validation.


{{% /notice %}}


## Pipeline Validation Workflow Integration

Here is the high-level logical flow of the Pipeline Validation module:

 ![PipelineValidationModule](/images/DeployOverwatch/Pipeline_Validation.png)


1. All workflow tasks of an Overwatch pipeline deployment run to
   completion.

2. An additional workflow task executes the Pipeline Validation
   module.

3. A health check report for the pipeline is generated with a record
   for each check that was performed.

4. Upon failure of any validation check, a snapshot of records that
   caused the failure are moved to a quarantine zone for debugging
   purpose.


## Validation Module Reports

Running the validation module produces two types of records:

* Health Check Reports
* Quarantine Reports


### Heath Check Report

Each validation run produces one or more records in the pipeline
health check report for each health check rule according to the number
of Overwatch ETL tables associated with that rule.  Each record
contains a reference to the corresponding Overwatch ETL run and its
snapshot timestamp.

The schema of a Health Check Report is as follows:

| Column Name             | Type   | Description                                                                                      | Example Value                                                              |
|-------------------------|----------|--------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| healthcheck_id        | uuid   | Unique ID for this Healthcheck_Report record                 | 272b83eb-1362-456a-8dae-26c3083181ac |
| etl_database          | string | Overwatch ETL database name                                    | ow_etl_azure_0721 |
| table_name            | string | Overwatch ETL table name                                       | clusterstatefact_gold |
| healthcheck_rule      | string | Description of health check rule applied to the table data      | Driver_Node_Type_ID Should Not be NULL |
| rule_type             | string | Type of validation rule (See enumerated values below.)         | Pipeline_Report_Validation |
| healthcheckMsg        | string | health check status of the table data for this health check rule | health check Failed: got 63 driver_node_type_ids which are null |
| overwatch_runID       | string | Overwatch run ID of the corresponding snapshot                 | b8985023d6ae40fa88bb6daef7f46725 |
| snap_ts               | long   | Snapshot timestamp in yyyy-mm-dd hh:mm:ss format             | 2023-09-27 07:43:39 |
| quarantine_id         | uuid   | Unique ID for Quarantine Report                                | cd6d7ecc-b72d-4c16-b8ca-01a6df4fba9c |


### Quarantine_Report

Table rows that do not satisfy the rules configured in the pipeline validation module
appear in the quarantine report using this schema:

| Column Name             | Type         | Description                                                                                                         | Example Value                                                                                       |
|-------------------------|--------------|---------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| quarantine_id         | uuid   | Unique ID for this Quarantine_Report record              | cd6d7ecc-b72d-4c16-b8ca-01a6df4fba9c |
| etl_database          | string | Overwatch ETL database name                                | ow_etl_azure_0721 |
| table_name            | string | Overwatch table name                                       | clusterstatefact_gold |
| healthcheck_rule_failed | string | Description of health check rule causing the row having keys to be quarantined | Driver_Node_Type_ID Should Not be NULL |
| rule_type             | string | Type of validation rule (See enumerated values below.)     | Pipeline_Report_Validation |
| healthcheck_type      | string | Denote the failure type of the health check rule.          | Warning or Failure |
| keys                  | string | JSON of primary (possibly compound) key value              | {"cluster_id":"0831-200003-6bhv8y0u","state":"TERMINATING","unixTimeMS_state_start":1693514245329} |
| snap_ts               | long   | Snapshot timestamp in yyyy-mm-dd hh:mm:ss format         | 2023-09-27 07:43:39 |


These reports are persisted to cloud storage as Delta tables.  The
output of the validation module reveals the location of these tables
as shown withing the stack trace in the following screenshot:

![Report StackTrace](/images/DeployOverwatch/StackTrace.png)


## Validation Module Rule Types

The rule_type field mentioned in the Heath Check Report schema above currently has three possible values:

1. Pipeline_Report_Validation
2. Single_Table_Validation
3. Cross_Table_Validation

By default the validation module applies health check rules of all three rule types.  Rules may applied selectively by providing arguments to the module.  More on this will be discussed in a later section.

The following sections describe each of these rule types in detail.


### Pipeline_Report_Validation

This rule type is validates records in the Overwatch pipeline_report table.

This rule type has two healthcheck_rule values:

* Check If Any Module is EMPTY
* Check If Any Module is FAILED


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

Different health check rules are defined for the following tables:


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

The health check rules for this rule type are applied to two or more tables. This rule type is used to 
validate data consistency across tables.

This rule type has the following healthcheck_rule values:

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


## How to run the Validation Module

Currently we can execute the validation module only running notebook.  Running through databricks workflow
would be added in the future.

Execution would be started upon calling PipelineValidation() function. Below are the input arguments of this function:
| Param                | Type    | Optional | Default Value | Description                                                                 |
|----------------------|---------|----------|---------------|-----------------------------------------------------------------------------|
| etlDB                | String  | No       | NA            | Overwatch ETL database name to validate |
| allRun               | Boolean | Yes      | true          | Validate all runs in pipeline_report?   |
| tableArray           | Array[String]  | Yes      | <empty array> | Subset of tables subject to Single Table Validation. Default means to validate all tables mentioned above. |
| crossTableValidation | Boolean | Yes      | true          | Perform Cross Table Validation? |

Here are the screenshots of Validation Module run with default setting and with custom configuration using input arguments:

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

### Validation Module run with specific tables mentioned in tableArray
![Table_Array](/images/DeployOverwatch/Table_Array.png)
Here we can see from the above screenshot that:
* By Default Pipeline_Report_Validation would be performed.
* Cross Table Validation would be performed
* Single table validation is performed on clusterstatefact_gold,jobruncostpotentialfact_gold as mentioned in tableArray param.

### Validation Module run with specific tables mentioned in tableArray and Disable Cross Validation
![cross_validation](/images/DeployOverwatch/cross_validation.png)
Here we can see from the above screenshot that:
* By Default Pipeline_Report_Validation would be performed.
* Single table validation is performed on clusterstatefact_gold,jobruncostpotentialfact_gold as mentioned in tableArray param.
* Cross Table Validation would not be performed as crossTableValidation flag us False here.

