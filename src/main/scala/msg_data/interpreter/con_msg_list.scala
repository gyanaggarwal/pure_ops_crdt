package msg_data
package interpreter

import model.Model._
import pure_ops._
import message._

object CONMSGLogInstances {
  implicit val uCONMSGLog: CONMSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new CONMSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
			val empty: CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
			  List.empty[MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]
		}
}