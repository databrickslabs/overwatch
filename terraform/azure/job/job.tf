# https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/job#parameters

resource "databricks_job" "overwatch" {
  name = "Overwatch"
  max_concurrent_runs = 1

  new_cluster  {
    autoscale {
      min_workers = var.min_node_count
      max_workers = var.max_node_count
    }
    spark_version = "7.6.x-scala2.12"
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
      etlDBName = var.overwatch_job_etl_dbname
      consumerDBName = var.overwatch_job_dbname
      ehName = var.overwatch_job_evh_name
      secretsScope = var.overwatch_job_secrets_scope
      ehKey = var.overwatch_job_secrets_evhub_key_name
      dbPATKey = var.overwatch_job_secrets_dbpat_key_name
      tempPath = var.overwatch_job_temppath
      maxDaysToLoad = var.overwatch_max_days_to_load
      primordialDateString = var.overwatch_primodial_date
      storagePrefix = var.storage_prefix
      scopes = "all"
    }
  }

  schedule {
    quartz_cron_expression = var.cron_expression
    timezone_id = var.timezone
  }

  library {
    maven {
      coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18"
    }
  }

  library {
    pypi {
      package = "plotly==4.8.2"
    }
  }

  library {
    maven {
      coordinates = "com.databricks.labs:overwatch_2.12:0.5.0.6"
    }
  }
}

# TODO: add permissions, etc.
