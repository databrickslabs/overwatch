package com.databricks.labs.overwatch.utils

import org.apache.spark.sql.Dataset

object TransformationDescriber {

  class NamedTransformation[T,U](
    val transformation: Dataset[T] => Dataset[U])(
    implicit name: sourcecode.Name) {

    // def name = name.value

    override def toString = s"NamedTransformation ${name.value}"

  }


  object NamedTransformation {

    def apply[T,U](
      transformation: Dataset[T] => Dataset[U])(
      implicit name: sourcecode.Name) =
      new NamedTransformation( transformation)( name)

  }


  implicit class TransformationDescriber[T,U]( ds: Dataset[T]) {
    
    def transformWithDescription[U](
      namedTransformation: NamedTransformation[T,U])(
      implicit
        // enclosing: sourcecode.Enclosing,
        name: sourcecode.Name,
        fileName: sourcecode.FileName,
        line: sourcecode.Line
    ): Dataset[U] = {

      // println( s"Inside TransformationDescriber.transformWithDescription: $enclosing")

      val callSite =  s"${name.value} at ${fileName.value}:${line.value}"

      val sc = ds.sparkSession.sparkContext
      sc.setJobDescription( namedTransformation.toString)
      sc.setCallSite( callSite)

      ds.transform( namedTransformation.transformation)

    }

  }

}
