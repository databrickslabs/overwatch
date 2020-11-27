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

variable "timezone" {
  description = "Timezone at which job will be executed"
  default = "UTC"
}

variable "cron_expression" {
  description = "Quartz cron expression for scheduling of the job"
}

variable "notebook_path" {
  description = "Path to the Overwatch notebook"
}

variable "overwatch_jar_path" {
  description = "Local path to the Overwatch jar"
}

variable "overwatch_remote_path" {
  description = "Path to the Overwatch jar on DBFS"
}
