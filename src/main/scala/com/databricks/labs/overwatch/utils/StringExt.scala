package com.databricks.labs.overwatch.utils

object StringExt {

  implicit class StringHelpers(s: String) {
    /**
     *Function will return if the column contains only allowable special character or not
     * @return Boolean
     */
    def containsNoSpecialChars: Boolean = {
      val pattern = "^[a-zA-Z0-9_\\s]*$"
      s.matches(pattern)
    }
  }

}
