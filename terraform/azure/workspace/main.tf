terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.2.8"
    }
  }
}

provider "azurerm" {
  version = "2.33.0"
  features {}
}

provider "databricks" {
#  profile = "az-bootcamp"
}

