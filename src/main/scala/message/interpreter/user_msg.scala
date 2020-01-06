package message
package interpreter

import model.Model._
import pure_ops._

object UserMSGInstances {
  implicit val uUserMSG: UserMSG[PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new UserMSG[PureOpsCRDT, UCRDT_ID, CRDTOps] {
		}
}