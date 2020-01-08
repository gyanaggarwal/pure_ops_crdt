package middleware

import model.Model._
import pure_ops._
import vector_clock._
import cluster_config._
import message._
import msg_data._
import po_log._
import util._

trait UpdateFromPeer[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def update(undeliv_msg_class: UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	           crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
						 nanyId: AnyId[NODE_ID],
						 canyId: AnyId[CLUSTER_ID])
	 					(implicit vectorClock: VectorClock[NODE_ID],
	 										tcsb: TCSB[NODE_ID, CLUSTER_ID],
	 										clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
											crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
	 										nodeVCLOCK: NodeVCLOCK[NODE_ID],
	 										msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 										msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 										msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 										msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 										pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 										polog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											crdtState: CRDTState[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  msgSndRcv: MSGSndRcv[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = crdt_state match {
		case _: TYPE_CRDT_STATE_DATA => check_and_upgrade(undeliv_msg_class, crdt_state, canyId)
		                                  .fold(crdt_state){case (crdt_state1, undeliv_msg1) => 
																				update(undeliv_msg1, crdt_state1, nanyId)}
		case _: TYPE_CRDT_STATE_NEW  => crdt_state
	}
	
	def update(undeliv_msg: UNDELIV_MSG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	           crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
					   anyId: AnyId[NODE_ID])
 	 					(implicit vectorClock: VectorClock[NODE_ID],
 	 										tcsb: TCSB[NODE_ID, CLUSTER_ID],
 	 										clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
											crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
 	 										nodeVCLOCK: NodeVCLOCK[NODE_ID],
 	 										msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
 	 										msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
 	 										msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
 	 										msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
 	 										pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
 	 										polog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
 											crdtState: CRDTState[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  crdtState.get_po_log(crdt_state)
		  .fold(crdt_state)(po_log => 
				crdtState.set_po_log(undeliv_msg.foldLeft(po_log){
					case (pl0, (crdt_instance, msg_list)) =>
					  pl0.updated(crdt_instance, update_msg_list(msg_list,
						                                           crdtState.get_po_log_class(crdt_instance,
																											                            pl0,
																																								  crdt_state,
																																								  anyId)))                       
				  },
				  crdt_state)  	
	)
	
	def check_and_upgrade(undeliv_msg_class: UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                      crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											  anyId: AnyId[CLUSTER_ID])
	 										 (implicit vectorClock: VectorClock[NODE_ID],
	 													     tcsb: TCSB[NODE_ID, CLUSTER_ID],
	 															 clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
	 															 nodeVCLOCK: NodeVCLOCK[NODE_ID],
	 															 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 															 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 															 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 															 msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 															 pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 														   polog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															   crdtState: CRDTState[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															   msgSndRcv: MSGSndRcv[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	Option[(CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	        UNDELIV_MSG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])] = {
	  val mcluster_detail = msgSndRcv.get_cluster_detail(undeliv_msg_class)
		val scluster_detail = crdtState.get_cluster_detail(crdt_state)
		val undeliv_msg = msgSndRcv.get_undeliv_msg(undeliv_msg_class)
		clusterConfig.comp_cc(mcluster_detail, scluster_detail, anyId) match {
			case EQCC => Some((crdt_state, undeliv_msg))
			case GTCC => Some((crdtState.upgrade_replica(mcluster_detail, crdt_state), undeliv_msg))
			case LTCC => Some((crdt_state, msgSndRcv.upgrade_replica(scluster_detail, undeliv_msg)))
			case IVCC => None
		}     	
	}
	
	def check_msg(msg_ops: MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	              po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							 (implicit vectorClock: VectorClock[NODE_ID],
							           tcsb: TCSB[NODE_ID, CLUSTER_ID],
											   nodeVCLOCK: NodeVCLOCK[NODE_ID],
											   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											   pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CHECKMSG = {
		val msg_check = tcsb.check_msg(msgOpr.get_node_id(msg_ops), 
		                               msgOpr.get_vclock(msg_ops), 
																	 pologClass.get_tcsb_class(po_log_class)) 
		msg_check match {
			case OOOMSG => msgLog.check_pending_msg(msg_ops, pologClass.get_msg_log(po_log_class))
			case _      => msg_check 
		}
	}	

	def merge_vc(node_vclock: NODE_VCLOCK[NODE_ID],
	             po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
						   mf: (NODE_ID,
							      VCLOCK[NODE_ID],
									  TCSB_CLASS[NODE_ID]) => TCSB_CLASS[NODE_ID])
              (implicit vectorClock: VectorClock[NODE_ID],
												tcsb: TCSB[NODE_ID, CLUSTER_ID],
											  nodeVCLOCK: NodeVCLOCK[NODE_ID],
												msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
  PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
  	pologClass.update_causal_stable(pologClass.set_tcsb_class(mf(nodeVCLOCK.get_node_id(node_vclock),
	                                                               nodeVCLOCK.get_vclock(node_vclock),
																									               pologClass.get_tcsb_class(po_log_class)),
																									            po_log_class))
	
	def update_msg_list(msg_list: MSG_LIST[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										 (implicit vectorClock: VectorClock[NODE_ID],
															 tcsb: TCSB[NODE_ID, CLUSTER_ID],
															 nodeVCLOCK: NodeVCLOCK[NODE_ID],
															 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															 msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															 pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  msg_list.foldLeft(po_log_class)((plc0, msg0) => 
		  msg0 match {
		  	case _: TYPE_MSG_OPS    => update_msg_ops(msgOpr.asMSG_OPS(msg0), plc0)
				case _: TYPE_MSG_VCLOCK => update_msg_vclock(msg0, plc0)
	}) 
																							 
	def update_msg_vclock(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
											 (implicit vectorClock: VectorClock[NODE_ID],
																 tcsb: TCSB[NODE_ID, CLUSTER_ID],
																 nodeVCLOCK: NodeVCLOCK[NODE_ID],
																 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																 msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																 pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  merge_vc(msgOpr.get_node_vclock(msg), po_log_class, tcsb.merge_peer)

	def update_msg_ops(msg_ops: MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										 po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										(implicit vectorClock: VectorClock[NODE_ID],
														  tcsb: TCSB[NODE_ID, CLUSTER_ID],
															nodeVCLOCK: NodeVCLOCK[NODE_ID],
															msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = check_msg(msg_ops, po_log_class) match {
		case NXTMSG => update_next_msg_ops(msg_ops, po_log_class)
		case OOOMSG => update_pending_msg_ops(msg_ops, po_log_class)
		case _      => po_log_class
	}

	def update_next_msg_ops(msg_ops: MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
											   (implicit vectorClock: VectorClock[NODE_ID],
																   tcsb: TCSB[NODE_ID, CLUSTER_ID],
																   nodeVCLOCK: NodeVCLOCK[NODE_ID],
																   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																   pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val (po_log_class1, opt_msg_ops1) = pologClass.take_pending_msg(msg_ops, 
			                                                              merge_vc(msgOpr.get_node_vclock(msg_ops), 
		                                                                         pologClass.update_msg(msg_ops, po_log_class),
															                                               tcsb.merge))
		opt_msg_ops1.fold(po_log_class1)(msg_ops1 => update_next_msg_ops(msg_ops1, po_log_class1))
	}
	
	def update_pending_msg_ops(msg_ops: MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												     po_log_class: PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
											      (implicit vectorClock: VectorClock[NODE_ID],
																      tcsb: TCSB[NODE_ID, CLUSTER_ID],
																      nodeVCLOCK: NodeVCLOCK[NODE_ID],
																      msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																      msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																      msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																      msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																      pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =  
	  merge_vc(msgOpr.get_node_vclock(msg_ops), 
		         pologClass.set_msg_log(msgLog.add_pending_msg(msg_ops, pologClass.get_msg_log(po_log_class)), po_log_class), 
						 tcsb.merge_peer_undelivered)
	
}

final case object UpdateFromPeer extends UpdateFromPeer[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
}