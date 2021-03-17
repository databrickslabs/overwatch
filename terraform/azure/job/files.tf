# https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/dbfs_file

resource "databricks_dbfs_file" "overwatch_jar" {
  source = pathexpand(var.overwatch_jar_path)
  content_b64_md5 = md5(filebase64(pathexpand(var.overwatch_jar_path)))
  path = var.overwatch_remote_path
  overwrite = true
  mkdirs = true
  validate_remote_file = true
}

# TODO: add permissions, etc.
