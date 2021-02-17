terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.3.0"
    }
  }
}

provider "databricks" {
}

