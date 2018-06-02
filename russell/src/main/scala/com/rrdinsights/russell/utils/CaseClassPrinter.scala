package com.rrdinsights.russell.utils


import scala.reflect.runtime.universe._

object CaseClassPrinter {

  final implicit class ProductPrinter[Z <: Product](clazz: Z) {
    def toPrettyString[T: TypeTag]: String =
      s"${extractClasName(clazz)}(${buildConstructor[T]})"

    private def buildConstructor[T: TypeTag]: String = {
      val fields = getFields[T]
      val values = clazz.productIterator.toIndexedSeq

      fields.zip(values).map(v => s"${v._1._1} = ${handleValue(v)}").mkString(", ")
    }

    private def handleValue(element: ((String, String), Any)): String = {
      if (element._1._2 == "String") {
        s""""${element._2}""""
      } else {
        s"${element._2}"
      }
    }


  }

  private def extractClasName[T <: Product](clazz: T): String = {
    clazz.getClass.toString.split('.').last.trim
  }


  private def mirror: Mirror = {
    runtimeMirror(Thread.currentThread().getContextClassLoader)
  }

  def localTypeOf[T: TypeTag]: `Type` = {
    val tag = implicitly[TypeTag[T]]
    tag.in(mirror).tpe.dealias
  }

  private def getFields[T: TypeTag]: IndexedSeq[(String, String)] = {
    typeOf[T].members.sorted.filter(!_.isMethod).map(v => (v.name.toString.trim(), v.typeSignature.dealias.toString)).toIndexedSeq
  }

}
