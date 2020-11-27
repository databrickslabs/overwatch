module "set_diagnostic" {
  source = "../../modules/azure-workspace-diag"

  # variables
  resource_group = "alexott-rg"
  region = "West Europe"
  evhub_ns_name = "overwatch-evhub-ns"
  evhub_name = "overwatch-evhub"
  workspace_name = "aott-db"
}
