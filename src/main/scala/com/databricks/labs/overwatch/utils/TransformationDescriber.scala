package com.databricks.labs.overwatch.utils

import org.apache.spark.sql.Dataset

// TODO: implement this as a `trait`.  Initial attempts would not
// compile because of the dependencies among other `trait`s and
// `object`s that would have to be refactored.

object TransformationDescriber {


  class NamedTransformation[T,U](
    val transformation: Dataset[T] => Dataset[U])(
    implicit _name: sourcecode.Name) {

    final val name: String = _name.value

    override def toString = s"${_name.value}: NamedTransformation"

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
