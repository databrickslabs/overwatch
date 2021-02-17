resource "databricks_job" "test_new" {
  name = "Seed on new cluster"

  new_cluster {
    num_workers   = 1
    spark_version = data.databricks_spark_version.latest_lts.id
    node_type_id  = data.databricks_node_type.smallest.id
  }

  notebook_task {
    notebook_path = databricks_notebook.test.path
  }

  email_notifications {}
}

resource "databricks_job" "test_existing" {
  name = "Seed on existing cluster"

  existing_cluster_id = databricks_cluster.test_cluster.id

  notebook_task {
    notebook_path = databricks_notebook.test.path
  }

  email_notifications {}
}
