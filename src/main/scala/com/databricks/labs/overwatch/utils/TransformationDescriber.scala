package com.databricks.labs.overwatch.utils

import org.apache.spark.sql.Dataset

object TransformationDescriber {

  class NamedTransformation[T,U](
    val transformation: Dataset[T] => Dataset[U])(
    implicit _name: sourcecode.Name) {

    def name = _name.value

    override def toString = s"${sourcecode.Name} ${_name.value}"

  }


  object NamedTransformation {

    def apply[T,U](
      t: Dataset[T] => Dataset[U])(
      implicit _name: sourcecode.Name) =
      new NamedTransformation( t)( _name)

  }


  implicit class TransformationDescriber[T,U]( ds: Dataset[T]) {
    
    def transformWithDescription[U](
      namedTransformation: NamedTransformation[T,U])(
      implicit
        // enclosing: sourcecode.Enclosing,
        _name: sourcecode.Name,
        _fileName: sourcecode.FileName,
        _line: sourcecode.Line
    ): Dataset[U] = {

      // println( s"Inside TransformationDescriber.transformWithDescription: $enclosing")

      val callSite =  s"${_name.value} at ${_fileName.value}:${_line.value}"

      val sc = ds.sparkSession.sparkContext
      sc.setJobDescription( namedTransformation.toString)
      sc.setCallSite( callSite)

      ds.transform( namedTransformation.transformation)

    }

  }

}
