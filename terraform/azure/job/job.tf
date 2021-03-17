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

    cluster_log_conf {
      dbfs {
        destination = "dbfs:/cluster-logs"
      }
    }
  }

  notebook_task {
    notebook_path = databricks_notebook.overwatch.path
    base_parameters = {
      etlDBName = var.overwatch_job_dbname
      presentationDBName = var.overwatch_job_dbname
      evhName = var.overwatch_job_evh_name
      secretsScope = var.overwatch_job_secrets_scope
      secretsEvHubKey = var.overwatch_job_secrets_evhub_key_name
      overwatchDBKey = var.overwatch_job_secrets_dbpat_key_name
      tempPath = var.overwatch_job_temppath
      maxDaysToLoad = var.overwatch_max_days_to_load
      primordialDateString = var.overwatch_primodial_date
    }
  }

  schedule {
    quartz_cron_expression = var.cron_expression
    timezone_id = var.timezone
  }

  library {
    maven {
      coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.18"
    }
  }

  library {
    pypi {
      package = "plotly==4.8.2"
    }
  }

  library {
    jar = "dbfs:${databricks_dbfs_file.overwatch_jar.path}"
  }
}

# TODO: add permissions, etc.
