data "azurerm_key_vault" "example" {
	name                = var.overwatch_job_secrets_scope
	resource_group_name = var.resource_group
}

# shouldnn't be executed by SP. Not required if secret scope is already provisioned
resource "databricks_secret_scope" "kv" {
	name = var.overwatch_job_secrets_scope

	keyvault_metadata {
    	resource_id = data.azurerm_key_vault.example.id
    	dns_name = data.azurerm_key_vault.example.vault_uri
	}
}

resource "databricks_token" "overwatch" {
	comment  = "Overwatch"
  	// 10 day token
	lifetime_seconds = 864000
}

resource "azurerm_key_vault_secret" "example" {
	name         = var.overwatch_job_secrets_dbpat_key_name
	value        = databricks_token.overwatch.token_value
	key_vault_id = data.azurerm_key_vault.example.id
}