package com.databricks.labs.overwatch.utils

object StringExt {

  implicit class StringHelpers(s: String) {
    /**
     *
     * @return Boolean
     */
    def containsNoSpecialChars: Boolean = {
      val pattern = "^[a-zA-Z0-9_]*$"
      s.matches(pattern)
    }
  }

}
