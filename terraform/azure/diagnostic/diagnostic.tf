module "set_diagnostic" {
  source = "../../modules/azure-workspace-diag"

  # variables
  resource_group = "some-rg"
  region = "UK South"
  evhub_ns_name = "overwatch2-evhub-ns"
  evhub_name = "overwatch2-evhub"
  workspace_name = "test-sp"
}
