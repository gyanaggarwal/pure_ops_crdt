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
					  msg_log: MSG_LOG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
					 (implicit msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
					           msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]): Any

	def valid_msg(user_msg: USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS])
							 (implicit crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
									 			 userMSG: UserMSG[CRDT_TYPE, CRDT_ID, CRDT_OPS]): Boolean

	def update_comm_crdt(msg_ops: MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
											(implicit msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 				  pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] 

	def causal_stable(msg_log: MSG_LOG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                  po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
									 (implicit msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													   pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	
	def update_msg(msg_ops: MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	               po_log_class0: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								(implicit vectorClock: VectorClock[NODE_ID],
								          nodeVCLOCK: NodeVCLOCK[NODE_ID],
												  msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val po_log_class1 = update_comm_crdt(msg_ops, po_log_class0)
	  pologClass.set_msg_log(msgLog.add_msg(msg_ops, pologClass.get_msg_log(po_log_class1)), po_log_class1)
	}	
	
	def update_causal_stable(po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
	                        (implicit vectorClock: VectorClock[NODE_ID],
													          tcsb: TCSB[NODE_ID, CLUSTER_ID],
																		nodeVCLOCK: NodeVCLOCK[NODE_ID],
																	  msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																		msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																	  msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																		msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																	  pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val (msg_log, csmsg_log) = msgLog.split_msg(tcsb.causal_stable(pologClass.get_tcsb_class(po_log_class)), 
		                                            pologClass.get_msg_log(po_log_class))
		causal_stable(csmsg_log, pologClass.set_msg_log(msg_log, po_log_class))
	}	
}

final case object UpdateCRDT extends UpdateCRDT[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
  def query(crdt_type: PureOpsCRDT,
						crdt_ops: CRDTOps,
					  crdt_data: Any,
					  msg_log: MSG_LOG[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
					 (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
					           msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]): 
	Any = crdt_type.eval(crdt_ops, crdt_data, msg_log)
	
	def valid_msg(user_msg: USER_MSG[PureOpsCRDT, UCRDT_ID, CRDTOps])
	             (implicit crdtInstance: CRDTInstance[PureOpsCRDT, UCRDT_ID],
							           userMSG: UserMSG[PureOpsCRDT, UCRDT_ID, CRDTOps]):
	Boolean = crdtInstance.get_crdt_type(userMSG.get_crdt_instance(user_msg)).valid_ops(userMSG.get_crdt_ops(user_msg))
	
	def update_comm_crdt(msg_ops: MSG_OPS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                     po_log_class: PO_LOG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
											(implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											          pologClass: POLogClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	PO_LOG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
	  pologClass.set_crdt_data(pologClass.get_crdt_type(po_log_class)
		  .update_comm_crdt(pologClass.get_crdt_data(po_log_class), msgOpr.get_crdt_ops(msg_ops)), po_log_class)
			
	def causal_stable(msg_log: MSG_LOG[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                  po_log_class: PO_LOG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
									 (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
													   msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
														 pologClass: POLogClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	PO_LOG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
    pologClass.set_crdt_data(pologClass.get_crdt_type(po_log_class)
	    .update_causal_stable(pologClass.get_crdt_data(po_log_class), msg_log), po_log_class)
}