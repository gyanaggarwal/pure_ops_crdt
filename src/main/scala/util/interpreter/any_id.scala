package util
package interpreter

import model.Model._

object AnyIdInstances {
  val intAnyId: AnyId[Int] =
    new AnyId[Int] {
      def eqv(v0: Int, v1: Int): Boolean = v0 == v1
    }
}

