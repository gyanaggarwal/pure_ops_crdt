package middleware

import model.Model._
import pure_ops._
import vector_clock._
import message._
import msg_data._
import po_log._

trait UpdateCRDT[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
  def query(crdt_type: CRDT_TYPE,
						crdt_ops: CRDT_OPS,
					  crdt_data: Any,
					  msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
					 (implicit msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
					           msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]): Any

	def valid_msg(user_msg: USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS])
							 (implicit crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
									 			 userMSG: UserMSG[CRDT_TYPE, CRDT_ID, CRDT_OPS]): Boolean
}

final case object UpdateCRDT extends UpdateCRDT[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
  def query(crdt_type: PureOpsCRDT,
						crdt_ops: CRDTOps,
					  crdt_data: Any,
					  msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
					 (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
					           msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]): 
	Any = crdt_type.eval(crdt_ops, crdt_data, msg_log)
	
	def valid_msg(user_msg: USER_MSG[PureOpsCRDT, UCRDT_ID, CRDTOps])
	             (implicit crdtInstance: CRDTInstance[PureOpsCRDT, UCRDT_ID],
							           userMSG: UserMSG[PureOpsCRDT, UCRDT_ID, CRDTOps]):
	Boolean = crdtInstance.get_crdt_type(userMSG.get_crdt_instance(user_msg)).valid_ops(userMSG.get_crdt_ops(user_msg))
}