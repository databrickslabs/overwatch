package com.databricks.labs.overwatch.utils

import org.scalatest.funspec.AnyFunSpec

class HelpersTest extends AnyFunSpec {

  describe("Helpers Test") {
    it("Numeric test"){
      assert(Helpers.isNumeric("1234") == true)
      assert(Helpers.isNumeric("abcd") == false)
    }
    it("Trailing slash test"){
      assert(Helpers.removeTrailingSlashes("http://hiii.com/") == "http://hiii.com")
      assert(Helpers.removeTrailingSlashes("http://hiii.com") == "http://hiii.com")
    }
    it("Duplicate slash test") {
      //dbfs
      assert(Helpers.removeDuplicateSlashes("dbfs:/mnt//cluster_logs/0310-182313-l2tbouaj") == "dbfs:/mnt/cluster_logs/0310-182313-l2tbouaj")
      assert(Helpers.removeDuplicateSlashes("dbfs://mnt//cluster_logs/0310-182313-l2tbouaj") == "dbfs:/mnt/cluster_logs/0310-182313-l2tbouaj")
      assert(Helpers.removeDuplicateSlashes("dbfs://mnt//cluster_logs/0310-182313-l2tbouaj/") == "dbfs:/mnt/cluster_logs/0310-182313-l2tbouaj/")
      //Azure
      assert(Helpers.removeDuplicateSlashes("abfss://LOCATION1//ABC//ccc") == "abfss://LOCATION1/ABC/ccc")
      assert(Helpers.removeDuplicateSlashes("abfss://LOCATION1/ABC//ccc") == "abfss://LOCATION1/ABC/ccc")
      assert(Helpers.removeDuplicateSlashes("abfss:/LOCATION1//ABC//ccc") == "abfss://LOCATION1/ABC/ccc")


      //FOR AWS
      assert(Helpers.removeDuplicateSlashes("s3a://databricks-field-eng-audit-logs/demo") == "s3a://databricks-field-eng-audit-logs/demo")
      assert(Helpers.removeDuplicateSlashes("s3a://databricks-field-eng-audit-logs//demo") == "s3a://databricks-field-eng-audit-logs/demo")
      assert(Helpers.removeDuplicateSlashes("s3a:/databricks-field-eng-audit-logs//demo") == "s3a://databricks-field-eng-audit-logs/demo")
      assert(Helpers.removeDuplicateSlashes("s3a:/databricks-field-eng-audit-logs//demo/") == "s3a://databricks-field-eng-audit-logs/demo/")

      assert(Helpers.removeDuplicateSlashes("s3://databricks-field-eng-audit-logs//demo/") == "s3://databricks-field-eng-audit-logs/demo/")
      assert(Helpers.removeDuplicateSlashes("s3://databricks-field-eng-audit-logs/demo") == "s3://databricks-field-eng-audit-logs/demo")
      assert(Helpers.removeDuplicateSlashes("s3://databricks-field-eng-audit-logs//demo/") == "s3://databricks-field-eng-audit-logs/demo/")
      assert(Helpers.removeDuplicateSlashes("s3:/databricks-field-eng-audit-logs//demo/") == "s3://databricks-field-eng-audit-logs/demo/")

      //FOR GCP
      assert(Helpers.removeDuplicateSlashes("gs://overwatch-global-gcp/test_mws") == "gs://overwatch-global-gcp/test_mws")
      assert(Helpers.removeDuplicateSlashes("gs://overwatch-global-gcp//test_mws") == "gs://overwatch-global-gcp/test_mws")
      assert(Helpers.removeDuplicateSlashes("gs://overwatch-global-gcp//test_mws/") == "gs://overwatch-global-gcp/test_mws/")
      assert(Helpers.removeDuplicateSlashes("gs:/overwatch-global-gcp//test_mws/") == "gs://overwatch-global-gcp/test_mws/")

      //HTTP
      assert(Helpers.removeDuplicateSlashes("http://databricks.com") == "http://databricks.com")
      assert(Helpers.removeDuplicateSlashes("http:/databricks.com") == "http://databricks.com")
      assert(Helpers.removeDuplicateSlashes("http:/databricks.com/") == "http://databricks.com/")


      assert(Helpers.removeDuplicateSlashes("https://databricks.com") == "https://databricks.com")
      assert(Helpers.removeDuplicateSlashes("https:/databricks.com") == "https://databricks.com")
      assert(Helpers.removeDuplicateSlashes("https://databricks.com/") == "https://databricks.com/")

    }
    it("test sanitizeURL"){
      assert(Helpers.sanitizeURL("https://databricks.com") == "https://databricks.com")
      assert(Helpers.sanitizeURL("https:/databricks.com") == "https://databricks.com")
      assert(Helpers.sanitizeURL("gs:/overwatch-global-gcp//test_mws/") == "gs://overwatch-global-gcp/test_mws")
      assert(Helpers.sanitizeURL("dbfs://mnt//cluster_logs/0310-182313-l2tbouaj/") == "dbfs:/mnt/cluster_logs/0310-182313-l2tbouaj")
      assert(Helpers.sanitizeURL("s3a:/databricks-field-eng-audit-logs//demo/") == "s3a://databricks-field-eng-audit-logs/demo")
      assert(Helpers.sanitizeURL("s3a://databricks-field-eng-audit-logs//demo/") == "s3a://databricks-field-eng-audit-logs/demo")
    }

  }

}
