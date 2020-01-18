package msg_data
package interpreter

import model.Model._
import pure_ops._

object CONMsgListInstances {
  implicit val uCONMsgList: CONMsgList[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new CONMsgList[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
			val empty: CON_MSG_LIST[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
			  List.empty[MSG_LIST[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]
		}
}