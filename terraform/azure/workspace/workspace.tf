data "azurerm_databricks_workspace" "example" {
  name                = var.workspace_name
  resource_group_name = var.resource_group
}
