// Albert Nogués. 23/05/2022. Automate Overwatch Depoloyment
// This file does all the overwatch prerequisites deployment

data "azurerm_storage_account" "owsa" {
  name                = azurerm_storage_account.owsa.name
  resource_group_name = azurerm_resource_group.rg.name
}

data "azurerm_storage_account" "logsa" {
  name                = azurerm_storage_account.logsa.name
  resource_group_name = azurerm_resource_group.rg.name
}

data "azurerm_databricks_workspace" "adb" {
  name                = azurerm_databricks_workspace.adb.name
  resource_group_name = azurerm_resource_group.rg.name
}

resource "random_string" "strapp" {
  length  = 5
  lower = true
  upper = false
  special = false
}

resource "azurerm_resource_group" "rg" {
  name = join("", [var.resource_group_name,random_string.strapp.result])
  location  = var.resource_group_location

  tags = {
    environment = "Overwatch"
  }
}

resource "azurerm_storage_account" "owsa" {
  name                     = join("", [var.overwatch_storage_account_name,random_string.strapp.result])
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  identity {
    type = "SystemAssigned"
  }

  tags = {
    environment = "Overwatch"
    purpose="Overwatch Delta Database"    
  }
}

resource "azurerm_storage_account" "logsa" {
  name                     = join("", [var.logs_storage_account_name,random_string.strapp.result])
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  identity {
    type = "SystemAssigned"
  }

  tags = {
    environment = "Overwatch"
    purpose="Overwatch Cluster Logs Storage"
  }
}

resource "azurerm_role_assignment" "data-contributor-role"{
  scope = azurerm_storage_account.owsa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id = var.service_principal_id_mount
}

resource "azurerm_role_assignment" "data-contributor-role-log"{
  scope = azurerm_storage_account.logsa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id = var.service_principal_id_mount
}

resource "azurerm_storage_data_lake_gen2_filesystem" "overwatch" {
  name               = "overwatch"
  storage_account_id = azurerm_storage_account.owsa.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "logs" {
  name               = "logs"
  storage_account_id = azurerm_storage_account.logsa.id
}

resource "azurerm_key_vault" "kv" {
  name                = join("", [var.key_vault_name,random_string.strapp.result])
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tenant_id           = var.tenant_id
  sku_name            = "standard"

  purge_protection_enabled = false
}


resource "azurerm_key_vault_access_policy" "storage" {
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = var.tenant_id
  object_id    = azurerm_storage_account.owsa.identity[0].principal_id

  key_permissions    = ["Get", "Create", "List", "Restore", "Recover", "UnwrapKey", "WrapKey", "Purge", "Encrypt", "Decrypt", "Sign", "Verify"]
  secret_permissions = ["Get", "List", "Set"]
}

resource "azurerm_key_vault_access_policy" "tfuser" {
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = var.tenant_id
  object_id    = var.user_id

  key_permissions    = ["Get", "Create", "List", "Restore", "Recover", "UnwrapKey", "WrapKey", "Purge", "Encrypt", "Decrypt", "Sign", "Verify"]
  secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
}

resource "azurerm_databricks_workspace" "adb" {
  name                = var.workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"

  tags = {
    Environment = "Overwatch"
  }
}


//EvenHub Part
resource "azurerm_eventhub_namespace" "ehn" {
  name                = join("", [var.evenhub_namespace_name,random_string.strapp.result])
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Basic"
  capacity            = 1

  tags = {
    environment = "Overwatch"
  }
}

data "azurerm_eventhub_namespace" "ehn" {
  name = azurerm_eventhub_namespace.ehn.name
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_eventhub" "eh" {
  name                = var.evenhub_name
  namespace_name      = azurerm_eventhub_namespace.ehn.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_eventhub_authorization_rule" "ehar" {
  name                = "overwatch"
  namespace_name      = azurerm_eventhub_namespace.ehn.name
  eventhub_name       = azurerm_eventhub.eh.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
  send                = true
  manage              = true
}

data "azurerm_eventhub_authorization_rule" "ehar" {
  name = azurerm_eventhub_authorization_rule.ehar.name
  resource_group_name = azurerm_resource_group.rg.name
  namespace_name      = azurerm_eventhub_namespace.ehn.name
  eventhub_name       = azurerm_eventhub.eh.name
}


resource "azurerm_eventhub_namespace_authorization_rule" "ehnar" {
  name                = "overwatch"
  namespace_name      = azurerm_eventhub_namespace.ehn.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
  send                = true
  manage              = true
}

data "azurerm_monitor_diagnostic_categories" "cat"{
  resource_id = azurerm_databricks_workspace.adb.id
}

resource "azurerm_monitor_diagnostic_setting" "ovwdgs" {
  name               = "OverwatchDGS"
  target_resource_id = azurerm_databricks_workspace.adb.id
  eventhub_name = azurerm_eventhub.eh.name
  eventhub_authorization_rule_id = azurerm_eventhub_namespace_authorization_rule.ehnar.id

  dynamic "log" {
    iterator = log_category
    for_each = data.azurerm_monitor_diagnostic_categories.cat.logs
    content {
      enabled = true
      category = log_category.value
      retention_policy {
        enabled = false
      }
    }
  }

  log{
    category = "clusters"
    enabled = true
    retention_policy {
      enabled = false
    }
  }
}