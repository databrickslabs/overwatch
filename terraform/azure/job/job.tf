# https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/job#parameters

resource "databricks_job" "overwatch" {
  name = "Overwatch"
  max_concurrent_runs = 1

  new_cluster  {
    autoscale {
      min_workers = var.min_node_count
      max_workers = var.max_node_count
    }
    spark_version = "6.4.x-scala2.11"
    node_type_id  = var.node_type
  }

  notebook_task {
    notebook_path = databricks_notebook.overwatch.path
  }

  # schedule {
  #   quartz_cron_expression = var.cron_expression
  #   timezone_id = var.timezone
  # }

  library {
    maven {
      coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17"
    }
  }

  library {
    maven {
      coordinates = "org.scalaj:scalaj-http_2.11:2.4.2"
    }
  }

  library {
    pypi {
      package = "plotly==4.8.2"
    }
  }

  library {
    jar = databricks_dbfs_file.overwatch_jar.path
  }
}

# TODO: add permissions, etc.
