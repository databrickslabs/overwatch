variable "node_type" {
  description = "What node type to use for workers"
  default = "Standard_DS3_v2"
}

variable "min_node_count" {
  description = "Minimum number of the worker nodes"
  default = 1
}

variable "max_node_count" {
  description = "Maximum number of the worker nodes"
}


variable "resource_group" {
  description = "Resource group name"
}

variable "timezone" {
  description = "Timezone at which job will be executed"
  default = "UTC"
}

variable "storage_prefix" {

}


variable "cron_expression" {
  description = "Quartz cron expression for scheduling of the job"
}

variable "notebook_path" {
  description = "Path to the Overwatch notebook"
}

variable "overwatch_job_secrets_scope" {
  description = "Name of the secret scope"
}

variable "overwatch_job_secrets_evhub_key_name" {
  description = "Secret key name for EventHubs connection string"
}

variable "overwatch_job_secrets_dbpat_key_name" {
  description = "Secret key name for DB PAT (personal access token)"
}

variable "overwatch_job_evh_name" {
  description = "Name of the EventHubs topic with diagnostic data"
}

variable "overwatch_job_dbname" {
  description = "Overwatch database name"
}

variable "overwatch_job_etl_dbname" {
  description = "Overwatch ETL database name"
}

variable "overwatch_job_temppath" {
  description = "Path on DBFS to store broken records, checkpoints, etc."
  default = "/tmp/overwatch"
}

variable "overwatch_primodial_date" {
  description = "Primodal data"
}

variable "overwatch_max_days_to_load" {
  description = "Max days to load"
}
