resource "databricks_notebook" "overwatch" {
  source = "../overwatch-azure.scala"
  path = var.notebook_path
}
