terraform {
  required_providers {
    azurerm = {
      version = "2.84.0"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

data "azurerm_resource_group" "example" {
  name     = var.resource_group
}

data "azurerm_client_config" "current" {
}