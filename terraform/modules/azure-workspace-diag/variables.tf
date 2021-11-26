variable resource_group {
  type        = string
  description = "Resource group to deploy"
}

variable region {
  type        = string
  description = "Region to deploy"
}

variable evhub_ns_name {
  type        = string
  description = "Name of eventhub namespace"
}

variable evhub_name {
  type        = string
  description = "Name of EventHubs topic"
}

variable workspace_name {
  type = string
  description = "The name of DB Workspace"
}

variable enabled_log_types {
  type        = list(string)
  description = "List of the log types to enable"
  default     = ["dbfs", "clusters", "accounts", "jobs", "notebook", "ssh",
    "workspace", "secrets", "sqlPermissions", "instancePools", "sqlanalytics", "genie", "globalInitScripts",
    "iamRole", "mlflowExperiment", "featureStore", "mlflowAcledArtifact", "RemoteHistoryService"]
}
