package middleware

import model.Model._
import pure_ops._
import cluster_config._
import vector_clock._
import message._
import msg_data._
import po_log._
import util._

trait UpdateFromUser[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def update_list(user_msg_list: List[USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS]],
	                crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
					        anyId: AnyId[NODE_ID])
						     (implicit crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
						               clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
										       userMSG: UserMSG[CRDT_TYPE, CRDT_ID, CRDT_OPS],
										       vectorClock: VectorClock[NODE_ID],
										       tcsb: TCSB[NODE_ID, CLUSTER_ID],
										       nodeVCLOCK: NodeVCLOCK[NODE_ID],
										       msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										       msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										       msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										       msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										       pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										       polog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										       crdtState: CRDTState[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										       updateCRDT: UpdateCRDT[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  user_msg_list.foldLeft(crdt_state)((csd, user_msg) => update(user_msg, csd, anyId))
	
	def update(user_msg: USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS],
	           crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
					   anyId: AnyId[NODE_ID])
						(implicit crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
						          clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
										  userMSG: UserMSG[CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  vectorClock: VectorClock[NODE_ID],
										  tcsb: TCSB[NODE_ID, CLUSTER_ID],
										  nodeVCLOCK: NodeVCLOCK[NODE_ID],
										  msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  polog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  crdtState: CRDTState[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  updateCRDT: UpdateCRDT[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  if (updateCRDT.valid_msg(user_msg)) {
		  crdtState.get_po_log(crdt_state)
			  .fold(crdt_state)(po_log => {
			  	val node_id = crdtState.get_node_id(crdt_state)
					val cluster_detail = crdtState.get_cluster_detail(crdt_state)
					val crdt_instance = userMSG.get_crdt_instance(user_msg)
					val po_log_class0 = crdtState.get_po_log_class(crdt_instance, po_log, crdt_state, anyId)
					val (po_log_class1, msg_ops1) = make_msg(node_id, user_msg, po_log_class0)
					crdtState.set_po_log(po_log.updated(crdt_instance, update_msg(msg_ops1, po_log_class1)), crdt_state)
			  })
	  } else crdt_state 

	def make_msg(node_id: NODE_ID,
						   user_msg: USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS],
						   po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							 (implicit userMSG: UserMSG[CRDT_TYPE, CRDT_ID, CRDT_OPS],
							           vectorClock: VectorClock[NODE_ID],
											   tcsb: TCSB[NODE_ID, CLUSTER_ID],
											   nodeVCLOCK: NodeVCLOCK[NODE_ID],
											   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											   pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]) = {
	  val (tcsb_class, vclock) = tcsb.next(node_id, pologClass.get_tcsb_class(po_log_class))
		val msg_ops = msgOpr.asMSG_OPS(msgOpr.make_user_msg_ops(nodeVCLOCK.make(node_id, vclock), user_msg))
		(pologClass.set_tcsb_class(tcsb_class, po_log_class), msg_ops) 	
	}
	
	def update_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	               po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								(implicit vectorClock: VectorClock[NODE_ID],
									        tcsb: TCSB[NODE_ID, CLUSTER_ID],
								          nodeVCLOCK: NodeVCLOCK[NODE_ID],
												  msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  pologClass.update_causal_stable(pologClass.update_msg(msg_ops, po_log_class))
}

final case object UpdateFromUser extends UpdateFromUser[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
}
