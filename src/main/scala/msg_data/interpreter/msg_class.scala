package msg_data
package interpreter

import model.Model._
import pure_ops._

object MSGClassInstances {
  implicit val uMSGClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
		}
}