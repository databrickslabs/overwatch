variable resource_group {
  type        = string
  description = "Resource group to deploy"
}

variable region {
  type        = string
  description = "Region to deploy"
}

variable name_prefix {
  type        = string
  description = "prefix to add to the names of resources, like, eventhubs, etc."
}

variable kv_name {
  type        = string
  description = "name of key vault"
}
