package com.databricks.labs.overwatch.utils

import org.apache.spark.sql.Dataset

object TransformationDescriber {

  class NamedTransformation[T,U](
    val t: Dataset[T] => Dataset[U])(
    implicit
      name: sourcecode.Name) {
    override def toString = name.value
  }

  implicit class TransformationDescriber[T,U]( ds: Dataset[T]) {
    
    def transformWithDescription[U](
      nt: NamedTransformation[T,U])(
      implicit
        enclosing: sourcecode.Enclosing,
        name: sourcecode.Name,
        fileName: sourcecode.FileName,
        line: sourcecode.Line
    ): Dataset[U] = {
      println( s"Inside TransformationDescriber.transformWithDescription: $enclosing")
      val sc = ds.sparkSession.sparkContext
      sc.setJobDescription( s"transformation $nt")
      sc.setCallSite( s"${name.value} at ${fileName.value}:${line.value}")
      ds.transform( nt.t)
    }

  }

}

