package msg_data

import model.Model._
import vector_clock._
import cluster_config._
import message._

trait MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def make(msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	         pending_msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = MSG_CLASS(msg_data, pending_msg_data)
	
	def empty(implicit msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = make(msgData.empty, msgData.empty)
	
	def get_msg_data(msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = msg_class.msg_data
	
	def get_pending_msg_data(msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = msg_class.pending_msg_data
	
	def set_msg_data(msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		               msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = msg_class.copy(msg_data = msg_data)

	def set_pending_msg_data(msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		                       msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = msg_class.copy(pending_msg_data = msg_data)
	
	def add_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	            msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						 (implicit vectorClock: VectorClock[NODE_ID],
						           nodeVCLOCK: NodeVCLOCK[NODE_ID],
											 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  set_msg_data(msgData.add_msg(msg_ops, get_msg_data(msg_class)), msg_class)
		
	def add_pending_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		                  msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							       (implicit vectorClock: VectorClock[NODE_ID],
							                 nodeVCLOCK: NodeVCLOCK[NODE_ID],
												       msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											         msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
		set_pending_msg_data(msgData.add_msg(msg_ops, get_pending_msg_data(msg_class)), msg_class)
		
	def merge(msg_class0: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		        msg_class1: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
					 (implicit msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  make(msgData.merge(get_msg_data(msg_class0), get_msg_data(msg_class1)),
	       msgData.merge(get_pending_msg_data(msg_class0), get_pending_msg_data(msg_class1)))
	
	def check_causal_stable(csvc: VCLOCK[NODE_ID],
			                    msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
			 	                 (implicit vectorClock: VectorClock[NODE_ID],
			 		                         nodeVCLOCK: NodeVCLOCK[NODE_ID],
			 						                 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	COMPVC = msgData.check_causal_stable(csvc, get_msg_data(msg_class))
					
	def check_pending_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		                    msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							         (implicit vectorClock: VectorClock[NODE_ID],
							                   nodeVCLOCK: NodeVCLOCK[NODE_ID],
												         msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											           msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CHECKMSG = msgData.check_msg(msg_ops, get_pending_msg_data(msg_class))

	def take_pending_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		                   msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							        (implicit vectorClock: VectorClock[NODE_ID],
							                  nodeVCLOCK: NodeVCLOCK[NODE_ID],
												        msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											          msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 Option[MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]) = {
		val (pending_msg_data, opt_msg_ops) = msgData.take_msg(msg_ops, get_pending_msg_data(msg_class))
		(set_pending_msg_data(pending_msg_data, msg_class), opt_msg_ops) 
	}

	def split_msg(csvc: VCLOCK[NODE_ID],
	              msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						   (implicit vectorClock: VectorClock[NODE_ID],
						             nodeVCLOCK: NodeVCLOCK[NODE_ID],
											   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										     msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]) = {
		val (msg_data, csmsg_data) = msgData.split_msg(csvc, get_msg_data(msg_class)) 
	  (set_msg_data(msg_data, msg_class), set_msg_data(csmsg_data, empty))
	}

  def remove_msg(rmsg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	               msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								 (implicit msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  set_msg_data(msgData.remove_msg(get_msg_data(rmsg_class), get_msg_data(msg_class)), msg_class)
	
  def undeliv_msg(logical_clock: LOGICAL_CLOCK,
	                msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								 (implicit msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  msgData.undeliv_msg(logical_clock, get_msg_data(msg_class))
	
  def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                    msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										 (implicit vectorClock: VectorClock[NODE_ID],
											         clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID], 
										           nodeVCLOCK: NodeVCLOCK[NODE_ID],
															 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val msg_data = msgData.upgrade_replica(cluster_detail, get_msg_data(msg_class))
		val pending_msg_data = msgData.upgrade_replica(cluster_detail, get_pending_msg_data(msg_class))
		set_msg_data(msg_data, set_pending_msg_data(pending_msg_data, msg_class))
	}
	
	def new_replica(msg_class: MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
	               (implicit msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  msg_class.copy(pending_msg_data = msgData.empty)
}