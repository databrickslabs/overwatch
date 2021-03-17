data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

data "databricks_node_type" "smallest" {
  local_disk = true
}

resource "databricks_cluster" "test_single_node" {
  cluster_name            = "Test Single Node"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 10
  spark_conf = {
    "spark.master" = "local[*]"
    "spark.databricks.cluster.profile" = "singleNode"
  }
  cluster_log_conf {
    dbfs {
      destination = "dbfs:/cluster-logs"
    }
  }
}

resource "databricks_cluster" "test_cluster" {
  cluster_name            = "Test Cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 20
  autoscale {
    min_workers = 1
    max_workers = 2
  }
  cluster_log_conf {
    dbfs {
      destination = "dbfs:/cluster-logs"
    }
  }

}

