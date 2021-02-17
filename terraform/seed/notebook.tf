data "databricks_current_user" "me" {}

resource "databricks_notebook" "test" {
  source = "./test.py"
  path = "${data.databricks_current_user.me.home}/SeedOverwatch"
}

