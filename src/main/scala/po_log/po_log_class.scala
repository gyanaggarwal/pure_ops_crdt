package po_log

import model.Model._
import vector_clock._
import cluster_config._
import message._
import msg_data._
import util._

trait POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def init_data(crdt_type: CRDT_TYPE): Any
	
	def make(tcsb_class: TCSB_CLASS[NODE_ID],
	         msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
				   crdt_type: CRDT_TYPE):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  PO_LOG_CLASS(tcsb_class, msg_log, crdt_type, init_data(crdt_type))
		
	def create(node_id: NODE_ID,
	           node_list: List[NODE_ID],
					   crdt_type: CRDT_TYPE,
					   anyId: AnyId[NODE_ID])
						(implicit vectorClock: VectorClock[NODE_ID],
										  tcsb: TCSB[NODE_ID, CLUSTER_ID],
										  msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  make(tcsb.create(node_id, node_list, anyId), msgLog.empty, crdt_type)
		
	def get_tcsb_class(po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	TCSB_CLASS[NODE_ID] = po_log_class.tcsb_class 

	def get_msg_log(po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = po_log_class.msg_log 

	def get_crdt_type(po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_TYPE = po_log_class.crdt_type 

	def get_crdt_data(po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	Any = po_log_class.crdt_data 

  def set_tcsb_class(tcsb_class: TCSB_CLASS[NODE_ID],
	                   po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = po_log_class.copy(tcsb_class = tcsb_class)						 

  def set_msg_log(msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = po_log_class.copy(msg_log = msg_log)						 

  def set_crdt_data(crdt_data: Any,
	                  po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = po_log_class.copy(crdt_data = crdt_data)						 

	def take_pending_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		                   po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							        (implicit vectorClock: VectorClock[NODE_ID],
										            nodeVCLOCK: NodeVCLOCK[NODE_ID],
												        msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												        msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												        msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															  msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 Option[MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]) = {
		val (msg_log, opt_msg_ops) = msgLog.take_pending_msg(msg_ops, get_msg_log(po_log_class))
		(set_msg_log(msg_log, po_log_class), opt_msg_ops)
	}

	def update_comm_crdt(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
											(implicit msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] 

	def causal_stable(msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                  po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
									 (implicit msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]

	def update_causal_stable(po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
	                        (implicit vectorClock: VectorClock[NODE_ID],
													          tcsb: TCSB[NODE_ID, CLUSTER_ID],
																		nodeVCLOCK: NodeVCLOCK[NODE_ID],
																	  msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																		msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																	  msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																		msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val (msg_log, csmsg_log) = msgLog.split_msg(tcsb.causal_stable(get_tcsb_class(po_log_class)), 
		                                            get_msg_log(po_log_class))
		causal_stable(csmsg_log, set_msg_log(msg_log, po_log_class))
	}

	def update_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	               po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								(implicit vectorClock: VectorClock[NODE_ID],
								          nodeVCLOCK: NodeVCLOCK[NODE_ID],
												  msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												  msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val po_log_class1 = update_comm_crdt(msg_ops, po_log_class)
	  set_msg_log(msgLog.add_msg(msg_ops, get_msg_log(po_log_class1)), po_log_class1)
	}	
	
  def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                    po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										 (implicit vectorClock: VectorClock[NODE_ID],
										           tcsb: TCSB[NODE_ID, CLUSTER_ID],
														   clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
														   nodeVCLOCK: NodeVCLOCK[NODE_ID],
														   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val tcsb_class = tcsb.upgrade_replica(cluster_detail, get_tcsb_class(po_log_class))
		val msg_log = msgLog.upgrade_replica(cluster_detail, get_msg_log(po_log_class))
		val po_log_class1 = set_tcsb_class(tcsb_class, set_msg_log(msg_log, po_log_class))
		
		cluster_detail match {
			case _: TYPE_CLUSTER_DETAIL_ADD => po_log_class1
			case _: TYPE_CLUSTER_DETAIL_RMV => update_causal_stable(po_log_class1)
		}
	}
	
	def new_replica(rnode_id: NODE_ID,
		              fnode_id: NODE_ID,
									po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								 (implicit tcsb: TCSB[NODE_ID, CLUSTER_ID],
									         msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val tcsb_class = tcsb.new_replica(rnode_id, fnode_id, get_tcsb_class(po_log_class))
		val msg_log = msgLog.new_replica(get_msg_log(po_log_class))
		set_tcsb_class(tcsb_class, set_msg_log(msg_log, po_log_class))
	}
}