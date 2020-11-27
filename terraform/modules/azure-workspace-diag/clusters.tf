# resource "databricks_cluster" "example" {
#   cluster_name            = "Test terraform"
#   spark_version           = "7.3.x-scala2.12"
#   node_type_id            = "Standard_F4s_v2"
#   autotermination_minutes = 20

#   autoscale {
#     min_workers = 1
#     max_workers = 2
#   }

#   cluster_log_conf {
#     dbfs {
#       destination = "dbfs:/cluster-logs"
#     }
#   }

#   library {
#     maven {
#       coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17"
#     }
#   }
# }
