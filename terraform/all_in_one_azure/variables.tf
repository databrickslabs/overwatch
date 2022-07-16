# Make sure to run this with an user account otherwise the create scope will fail because SPN auth is not supported
# Specific azure account variables. You can fill these here with the default keyworkd or fill it in a terraform.tfvars file
variable "tenant_id" {
    description = "The tenant ID of our AAD account"
}

variable "subscription_id" {
    description = "The ID of the azure subscription where we will be deploying overwatch"
}

variable "service_principal_id_mount" {
    description = "ObjectID of the service principal that will be granted blob contributor role in the storage account of both logs and overwatch DB. This has to be the objectID of the service principal that will be used to mount the ADLS in the DBFS"
}

variable "user_id" {
    description = "ObjectID of the user that will launch this terraform deployment. TO not have issues adding secrets in the KV we will set a policy for ths user"
}

variable "overwatch_spn"{
    description = "ApplicationID of the SPN that will be used for Overwatch"
}

variable "overwatch_spn_pass"{
    description = "Password of the ApplicationID of the SPN that will be used for Overwatch"
}

# Azure Overwatch prerequisites specific variables
variable "resource_group_name" {
    default = "overwatch"
    description = "Main Resource Group Name"
}

variable "resource_group_location" {
  default = "westeurope"
  description   = "Location of the resource group"
}

variable "overwatch_storage_account_name" {
    default = "overwatchdb"
    description = "Main DataLake name"
}

variable "logs_storage_account_name" {
    default = "overwatchlogs"
    description = "Main DataLake name"
}

variable "key_vault_name" {
    default = "overwatchkv"
    description = "Main Keyvault Name"
}

variable "evenhub_namespace_name" {
    default = "overwatch"
    description = "Eventhub Namespace Name"
}

variable "evenhub_name" {
    default = "shared-databricks-ds"
    description = "Eventhub Databricks"
}

variable "workspace_name"{
    default = "DatabricksOverwatch"
    description = "Name of the Azure Databricks Workspace"
}

# Overwatch specific variables
variable "overwatch_home_dir" {
    default = "/Overwatch"
    description = "Overwatch home directory in the workspace"
}

variable "overwatch_job_name"{
    default = "Overwatch Job"
    description = "Overwatch Job Name"  
}

variable "overwatch_job_notification_email"{
    default = "your@email.com"
    description = "Overwatch Job Notification Email"  
}

variable "logs_dbfs_mount_point" {
    default = "dbfs:/mnt/logs"
    description = "Path in dbfs where the logs storage account will be mounted"    
}

variable "overwatch_mount_point" {
    default = "/mnt/overwatch"
    description = "Path in the driver where the Overwatch storage account will be mounted"    
}

variable "overwatch_job_scopes" {
    default = "jobs,clusters,clusterEvents,sparkEvents,audit,notebooks,accounts"
    description = "All the scopes you want to read for overwatch. Check documentation for more details. We are not using pools"
}

variable "overwatch_primordial_date" {
    default = "2022-03-08"
    description = "The initial date load for overwatch in string format YYYY-MM-DD"
}

variable "cron_job_schedule" {
    default = "0 0 8 * * ?"
    description = "Cron expression to schedule the Overwatch Job"
}

variable "cron_timezone_id" {
    default = "Europe/Brussels"
    description = "Timezone for the cron schedule. Check documentation about supported timezone formats"
}