terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.3.11"
    }
    azurerm = {
      version = "2.84.0"
    }
  }
}

provider "azurerm" {
  features {
  }
}

provider "databricks" {
  host = "..."
}

data "databricks_current_user" "me" {
}

