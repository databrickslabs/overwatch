// Albert Nogués. 23/05/2022. Automate Overwatch Depoloyment
// This file does all the overwatch deployment automatically

//Upload Databricks notebook
resource "databricks_notebook" "overwatch_notebook" {
  source = "notebooks/OverwatchSource.dbc"
  path   = var.overwatch_home_dir
  format = "DBC"
}

resource "databricks_secret_scope" "overwatch" { //ONLY WORKS WITH TERRAFORM AZ-CLI LOGIN!!! NO SPN!!!!
  name                     = "overwatch"
  initial_manage_principal = "users"

  keyvault_metadata {
    resource_id = azurerm_key_vault.kv.id
    dns_name    = azurerm_key_vault.kv.vault_uri
  }
}

resource "azurerm_key_vault_secret" "clientid"{
  name                     = "OVERWATCH-CLIENT-ID"
  value                    = var.overwatch_spn
  expiration_date          = "2030-12-31T23:59:59Z"
  key_vault_id             = azurerm_key_vault.kv.id
  depends_on = [
    azurerm_key_vault_access_policy.tfuser
  ]
}

resource "azurerm_key_vault_secret" "clientsecret"{
  name                     = "OVERWATCH-CLIENT-SECRET"
  value                    = var.overwatch_spn_pass
  expiration_date          = "2030-12-31T23:59:59Z"  
  key_vault_id             = azurerm_key_vault.kv.id
  depends_on = [
    azurerm_key_vault_access_policy.tfuser
  ]
}

resource "azurerm_key_vault_secret" "tenant"{
  name                     = "OVERWATCH-TENANT"
  value                    = var.tenant_id
  expiration_date          = "2030-12-31T23:59:59Z"  
  key_vault_id             = azurerm_key_vault.kv.id
  depends_on = [
    azurerm_key_vault_access_policy.tfuser
  ]
}

resource "azurerm_key_vault_secret" "owsa"{
  name                     = "OVERWATCH-STORAGE-ACCOUNT"
  value                    = azurerm_storage_account.owsa.name
  expiration_date          = "2030-12-31T23:59:59Z"  
  key_vault_id             = azurerm_key_vault.kv.id
  depends_on = [
    azurerm_key_vault_access_policy.tfuser
  ]
}

resource "azurerm_key_vault_secret" "logsa"{
  name                     = "OVERWATCH-LOG-STORAGE-ACCOUNT"
  value                    = azurerm_storage_account.logsa.name
  expiration_date          = "2030-12-31T23:59:59Z"  
  key_vault_id             = azurerm_key_vault.kv.id
  depends_on = [
    azurerm_key_vault_access_policy.tfuser
  ]
}

resource "azurerm_key_vault_secret" "ehconnectionstring"{
  name                     = "EH-CONNECTION-STRING"
  value                    = azurerm_eventhub_authorization_rule.ehar.primary_connection_string
  expiration_date          = "2030-12-31T23:59:59Z"  
  key_vault_id             = azurerm_key_vault.kv.id
  depends_on = [
    azurerm_key_vault_access_policy.tfuser
  ]
}

resource "databricks_token" "pat" {
  comment = "ADB-PAT for Overwatch"
}

resource "azurerm_key_vault_secret" "adbpat"{
  name                     = "ADB-PAT"
  value                    = databricks_token.pat.token_value
  expiration_date          = "2030-12-31T23:59:59Z"  
  key_vault_id             = azurerm_key_vault.kv.id
  depends_on = [
     azurerm_key_vault_access_policy.tfuser,
    databricks_token.pat
  ]
}

data "databricks_node_type" "smallest" {
  local_disk = true //If we uncomment this we will get a Standard_E4s_v4 instance a shade more expensive but with 32 gb of ram instead of the 8 of the Standard_F4s so may be worth it if you need more power
  depends_on = [azurerm_databricks_workspace.adb]
}

// Latest LTS version
data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  depends_on = [azurerm_databricks_workspace.adb]
}

//First of all we require a dummy job otherwise first run of overwatch will crash reading info of jobs launched. 
//WE NEEED TO RUN THIS JOB BEFORE OVERWATCH RUNS
resource "databricks_job" "dummyjob" {
  name = "Dummy Job"
  new_cluster{
    num_workers = 0
    spark_version           = data.databricks_spark_version.latest_lts.id
    node_type_id            = data.databricks_node_type.smallest.id
    cluster_log_conf {
      dbfs {
      destination = var.logs_dbfs_mount_point
      }
    }
    spark_conf = {
      # Single-node
      "spark.databricks.cluster.profile" : "singleNode"
      "spark.master" : "local[*]"
    }
    custom_tags =  {"ResourceClass" : "SingleNode"}

  }
  notebook_task {
    notebook_path = join("/", [databricks_notebook.overwatch_notebook.path,"Dummy"])
  }
}

resource "databricks_job" "overwatch" {
  name = var.overwatch_job_name
  new_cluster{
    num_workers = 0
    spark_version           = data.databricks_spark_version.latest_lts.id
    node_type_id            = "Standard_DS4_v2"

    cluster_log_conf {
      dbfs {
      destination = var.logs_dbfs_mount_point
      }
    }

    spark_conf = {
      # Single-node
      "spark.databricks.cluster.profile" : "singleNode"
      "spark.master" : "local[*]"
    }
    custom_tags =  {"ResourceClass" : "SingleNode"}    
  }
  notebook_task {
    notebook_path = join("/", [databricks_notebook.overwatch_notebook.path,"Overwatch - Job"])
    base_parameters = {
      "consumerDBName": "overwatch",
      "secretsScope": "overwatch",
      "scopes": var.overwatch_job_scopes,
      "maxDaysToLoad": "1500",
      "etlDBName": "overwatch_etl",
      "dbPATKey": azurerm_key_vault_secret.adbpat.name,
      "ehName": var.evenhub_name,
      "primordialDateString": var.overwatch_primordial_date,
      "ehKey": azurerm_key_vault_secret.ehconnectionstring.name,
      "storagePrefix": var.overwatch_mount_point
    }
  }
  library {
    maven {
    coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21"
    exclusions  = []
    }
  }
  library{
    maven {
    coordinates = "com.databricks.labs:overwatch_2.12:0.6.1.0"
    exclusions  = []
    }
  }
  email_notifications {

    on_failure = [var.overwatch_job_notification_email]
    no_alert_for_skipped_runs = false

  }

  schedule{
    quartz_cron_expression = var.cron_job_schedule
    timezone_id = var.cron_timezone_id
    pause_status = "UNPAUSED"
  }
}
