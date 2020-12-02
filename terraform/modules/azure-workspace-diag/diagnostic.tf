# https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/monitor_diagnostic_setting

data "azurerm_eventhub_namespace_authorization_rule" "overwatch" {
  name                = "overwatch"
  resource_group_name = var.resource_group
  namespace_name      = var.evhub_ns_name
}

resource "azurerm_monitor_diagnostic_setting" "overwatch" {
  name               = "overwatch"
  target_resource_id = data.azurerm_databricks_workspace.example.id
  eventhub_authorization_rule_id = data.azurerm_eventhub_namespace_authorization_rule.overwatch.id
  eventhub_name = var.evhub_name

  dynamic "log" {
    for_each = var.enabled_log_types
    content {
      category = log.value
      enabled = true
    }
  }
  
}
