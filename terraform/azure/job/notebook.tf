resource "databricks_notebook" "overwatch" {
  content = filebase64("../overwatch-azure.scala")
  path = var.notebook_path
  overwrite = false
  mkdirs = true
  language = "SCALA"
  format = "SOURCE"
}


# TODO: add permissions, etc.
