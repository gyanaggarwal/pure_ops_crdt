package message
package interpreter

import model.Model._
import pure_ops._

object CRDTInstances {
  implicit val uCRDTInstance: CRDTInstance[PureOpsCRDT, UCRDT_ID] =
    new CRDTInstance[PureOpsCRDT, UCRDT_ID] {
		}
}
