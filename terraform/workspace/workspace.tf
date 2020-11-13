# terraform import azurerm_databricks_workspace.example /subscriptions/6369c148-f8a9-4fb5-8a9d-ac1b2c8e756e/resourceGroups/azure-bootcamp/providers/Microsoft.Databricks/workspaces/az-bootcamp-aott-workspace

# resource "azurerm_databricks_workspace" "example" {
#   name                = "az-bootcamp-aott-workspace"
#   resource_group_name = "azure-bootcamp"
#   location            = "UK South"
#   sku                 = "premium"
# }

# data "azurerm_databricks_workspace" "example" {
#   name                = "aott-db"
#   resource_group_name = "alexott-rg"
# }

# output "databricks_workspace_id" {
#   value = azurerm_databricks_workspace.example.workspace_id
# }
