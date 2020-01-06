package message
package interpreter

import model.Model._
import pure_ops._

object MSGOperationInstances {
  implicit val uMSGOperation: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
		  val empty_msg_list: 
			  MSG_LIST[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
				List.empty[MESSAGE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]
		}
}