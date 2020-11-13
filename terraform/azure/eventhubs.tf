# https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/eventhub_namespace

resource "azurerm_eventhub_namespace" "overwatch" {
  name                = "${var.name_prefix}-evhub-ns"
  location            = var.region
  resource_group_name = var.resource_group
  sku                 = "Standard"
  capacity            = 2
}

resource "azurerm_eventhub" "overwatch" {
  name                = "${var.name_prefix}-evhub"
  namespace_name      = azurerm_eventhub_namespace.overwatch.name
  resource_group_name = var.resource_group
  partition_count     = 32
  message_retention   = 2
}

# do we need it?
resource "azurerm_eventhub_authorization_rule" "overwatch_listen" {
  name                = "${var.name_prefix}-evhub-rule-listen"
  namespace_name      = azurerm_eventhub.overwatch.namespace_name
  resource_group_name = azurerm_eventhub.overwatch.resource_group_name
  eventhub_name       = azurerm_eventhub.overwatch.name

  listen = true
  send   = false
  manage = false
}

output "eventhub_conn_read" {
  value = azurerm_eventhub_authorization_rule.overwatch_listen.primary_connection_string
}
