---
title: "Configuration"
date: 2022-12-12T11:35:40-05:00
weight: 1
---
## This section will walk you through the steps necessary to configure Overwatch.

### How it works
Overwatch deployment is driven by a configuration file which will be in csv format. This csv file will contain all the necessary details to perform the deployment

Please follow the below steps to create a config.csv.
### Creating Config.csv file

Please use the template [advance_mws_config.csv](/assets/DeployOverwatch/mws_advance_config.csv).

### Column description

Column | Type    | IsRequired     | Description
:------|:--------|:---------------|:----------------
workspace_name| String  | True           |Name of the workspace.
workspace_id| String  | True           |Id of the workspace.
workspace_url| String  | True           |URL of the workspace.
api_url| String  | True           |API URL for the Workspace (execute in scala dbutils.notebook.getContext().apiUrl.get to get the API URL for the workspace.NOTE: Workspace_URL and API_URL can be different for a workspace).
cloud| String  | True           |Cloud provider( Azure or AWS).
primordial_date| String  | True           |The date from which Overwatch will capture the details(The format should be yyyy-MM-dd ex:2022-05-20).
etl_storage_prefix	| String  | True           |The location on which Overwatch will store the data.
etl_database_name| String  | True           |The name of the ETL data base for Overwatch.
consumer_database_name| String  | True           |The name of the Consumer database for Overwatch.
secret_scope	| String  | True           |Secrate scope name(This should be created on the workspace on which Overwatch job will execute).
secret_key_dbpat	| String  | True           | This will contain the PAT token of the workspace(The key should be present in the secret_scope).
auditlogprefix_source_aws| String  | True for AWS   |Location of auditlog (Only for AWS).
eh_name| String  | True for Azure | Event hub name (Only for Azure ,The event hub will contain the audit logs of the workspace)
eh_scope_key	| String  | True for Azure           |Key to connect with event hub(Only for Azure ,this key should be present in the secret scope)
interactive_dbu_price	| Double  | True           |Price of the interactive dbu.
automated_dbu_price	| Double  | True           |Price of the automated dbu.
sql_compute_dbu_price| Double  | True           |Price of the sql compute dbu.
jobs_light_dbu_price	| Double  | True           | Price of the jobs light dbu.
max_days| Integer | True           |Incase of very old primordial date the max_days will be used to batch the data(Recommendation: 30)
excluded_scopes	| String  | False          |Scopes that should not be performed in the run for that workspace.You can choose between audit,sparkEvents,jobs,clusters,clusterEvents,notebooks,pools,accounts,dbsql(scopes should be separated by “:” .Note:Please don't fill this column if you want to run all the scopes)
active| Boolean | True           |If active this config row will be considered for deployment.
proxy_host	| String  | False          |Proxy url for the workspace.
proxy_port	| String  | False          |Proxy port for the workspace
proxy_user_name	| String  | False          |Proxy user name for the workspace.
proxy_password_scope	| String  | False          |Scope which contains the proxy password key.
proxy_password_key| String  | False          |Key which contains proxy password. 
success_batch_size	| Integer | False          |Overwatch makes multiple api calls and stores the interim results to a temp location, success_batch_size indicates the  size of the buffer on filling of which the result will be written to a temp location.This is used to boost the performance.Default value:200.
error_batch_size	| Integer | False          |Overwatch makes multiple api calls and stores the interim results to a temp location error_batch_size indicates the  size of the buffer which contains the error incase of the api call failure on filling of which the result will be written to a temp location.This is used to boost the performance.Default value:500.
enable_unsafe_SSL	| Boolean | False          |Enables unsafe ssl.Default value:False.
thread_pool_size	| Integer | False          |Overwatch makes multiple api calls in parallel thread_pool_size indicates the level of parallelism.Default value:4.
api_waiting_time	| Long    | False          |Overwatch makes multiple async api calls in parallel api_waiting_time signifies the waiting time incase of no response received from the api call.Default value:300000.(5 minutes)


Document [OverwatchMultiworkspaceUserGuide.docx](/assets/DeployOverwatch/mws_guide.docx) will explain all the column details present in  [advance_mws_config.csv](/assets/DeployOverwatch/mws_advance_config.csv).
