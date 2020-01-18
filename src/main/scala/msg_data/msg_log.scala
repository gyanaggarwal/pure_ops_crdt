package msg_data

import model.Model._
import vector_clock._
import cluster_config._
import message._

trait MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def empty: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	
	def get_msg_class(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                  msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
									 (implicit nodeVCLOCK: NodeVCLOCK[NODE_ID],
													   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										         msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(NODE_ID, 
	 MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]) = {
 		val mnode_id = msgOpr.get_node_id(msg_ops)
	  (mnode_id, msg_log.get(mnode_id).fold(msgClass.empty)(msg_class => msg_class))
	}
		
  def add_msg_data(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                 msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									 amsg: (MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									        MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]) =>
													MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
									(implicit vectorClock: VectorClock[NODE_ID],
									          nodeVCLOCK: NodeVCLOCK[NODE_ID],
													  msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													  msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													  msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val (mnode_id, msg_class) = get_msg_class(msg_ops, msg_log)
		msg_log.updated(mnode_id, amsg(msg_ops, msg_class))
	}
	
  def add_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	            msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						 (implicit vectorClock: VectorClock[NODE_ID],
									     nodeVCLOCK: NodeVCLOCK[NODE_ID],
											 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  add_msg_data(msg_ops, msg_log, msgClass.add_msg)

	def add_msg(msg_list: MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		          msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						 (implicit vectorClock: VectorClock[NODE_ID],
										   nodeVCLOCK: NodeVCLOCK[NODE_ID],
											 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
		msg_list.foldLeft(msg_log)((ml, msg) => add_msg(msgOpr.asMSG_OPS(msg), ml))

	def add_pending_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		                  msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							       (implicit vectorClock: VectorClock[NODE_ID],
										           nodeVCLOCK: NodeVCLOCK[NODE_ID],
												       msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												       msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												       msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
		add_msg_data(msg_ops, msg_log, msgClass.add_pending_msg)
		
	def check_pending_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		                    msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							         (implicit vectorClock: VectorClock[NODE_ID],
										             nodeVCLOCK: NodeVCLOCK[NODE_ID],
												         msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												         msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												         msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CHECKMSG = {
	  val (_, msg_class) = get_msg_class(msg_ops, msg_log)
		msgClass.check_pending_msg(msg_ops, msg_class)															 	
	}

	def take_pending_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		                   msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							        (implicit vectorClock: VectorClock[NODE_ID],
										            nodeVCLOCK: NodeVCLOCK[NODE_ID],
												        msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												        msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												        msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 Option[MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]) = {
	  val (mnode_id, msg_class0) = get_msg_class(msg_ops, msg_log)
		val (msg_class1, opt_msg_ops) = msgClass.take_pending_msg(msg_ops, msg_class0)
		(msg_log.updated(mnode_id, msg_class1), opt_msg_ops)															 	
	}

  def split_msg(csvc: VCLOCK[NODE_ID],
	              msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						    (implicit vectorClock: VectorClock[NODE_ID],
									        nodeVCLOCK: NodeVCLOCK[NODE_ID],
											    msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											    msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											    msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
  (MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]) = 
	  msg_log.foldLeft((empty, empty)){case ((ml1, ml2), (ni0, mc0)) => {
	  	val (mc1, mc2) = msgClass.split_msg(csvc, mc0)
			(ml1.updated(ni0, mc1), ml2.updated(ni0, mc2))
	  }
	} 
	
	def remove_msg(rmsg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	               msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								(implicit msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
								          msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  rmsg_log.foldLeft(msg_log){
	  	case (ml0, (rn0, rmc0)) => ml0.get(rn0)
			  .fold(ml0)(mc0 => ml0.updated(rn0, msgClass.remove_msg(rmc0, mc0)))
	  }
	
	def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
		                  msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							       (implicit vectorClock: VectorClock[NODE_ID],
												       clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
										           nodeVCLOCK: NodeVCLOCK[NODE_ID],
												       msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												       msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												       msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
    msg_log.mapValues(msg_class => msgClass.upgrade_replica(cluster_detail, msg_class))

	def new_replica(msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							   (implicit msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  msg_log.mapValues(msg_class => msgClass.new_replica(msg_class))
}