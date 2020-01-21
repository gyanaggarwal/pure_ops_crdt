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
				
				def isConcurrent(crdt_type: PureOpsCRDT,
				                 msg_ops0: MSG_OPS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   msg_ops1: MSG_OPS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
												(implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]): 
				Boolean = crdt_type.isConcurrent(msgOpr.get_crdt_ops(msg_ops0), msgOpr.get_crdt_ops(msg_ops1))

		}
}