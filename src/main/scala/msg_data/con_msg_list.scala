package msg_data

import model.Model._
import cluster_config._
import vector_clock._
import message._

trait CONMSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def empty: CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	
  def msg_log(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						 (implicit vectorClock: VectorClock[NODE_ID],
									     nodeVCLOCK: NodeVCLOCK[NODE_ID],
											 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  msgLog.add_msg(msg_ops, msgLog.empty)
		
	def check_con(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
							  msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
							  cf: (CRDT_OPS, CRDT_OPS) => Boolean)
							 (implicit vectorClock: VectorClock[NODE_ID],
							           nodeVCLOCK: NodeVCLOCK[NODE_ID],
											   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	COMPVC = {
		val vclock = msgOpr.get_vclock(msg_ops)
		val crdt_ops = msgOpr.get_crdt_ops(msg_ops)
		msg_log.filterKeys(_ != msgOpr.get_node_id(msg_ops)).foldLeft(UNDVC: COMPVC){
		  case (CONVC, _) => CONVC
		  case (_, (_, msg_class)) => {
		  	val msg_data = msgClass.get_msg_data(msg_class)
				msg_data.foldLeft(UNDVC: COMPVC){
					case (CONVC, _)         => CONVC
					case (_, (_, msg_ops0)) => vectorClock.comp_vc(vclock, msgOpr.get_vclock(msg_ops0)) match {
            case CONVC => cf(crdt_ops, msgOpr.get_crdt_ops(msg_ops0)) match {
							case true  => CONVC
							case false => UNDVC
						}
						case _     => UNDVC
					}
				}
		  }
	  }
	}

	def add_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	            con_msg_log: CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
						  cf: (CRDT_OPS, CRDT_OPS) => Boolean)
						 (implicit vectorClock: VectorClock[NODE_ID],
						           nodeVCLOCK: NodeVCLOCK[NODE_ID],
										   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val (cml, ml) = con_msg_log.foldLeft((empty, msg_log(msg_ops))){
			case ((cml0, ml0), mlx0) => check_con(msg_ops, mlx0, cf) match {
				case CONVC => (cml0, msgLog.merge(ml0, mlx0))
				case _     => (mlx0 :: cml0, ml0)
			}
		}
		ml :: cml
	}
							 			 
	def split_msg(csvc: VCLOCK[NODE_ID],
	              con_msg_log: CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						   (implicit vectorClock: VectorClock[NODE_ID],
						             nodeVCLOCK: NodeVCLOCK[NODE_ID],
										     msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]) = 
		con_msg_log.foldLeft((empty, msgLog.empty)){
			case ((cml0, ml0), mlx0) => msgLog.check_causal_stable(csvc, mlx0) match {
				case LTVC => (cml0, msgLog.merge(mlx0, ml0))
				case EQVC => (cml0, msgLog.merge(mlx0, ml0))
				case _    => (mlx0 :: cml0, ml0)
			}
	}
	
	def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                    con_msg_log: CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										 (implicit vectorClock: VectorClock[NODE_ID],
										           clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
														   nodeVCLOCK: NodeVCLOCK[NODE_ID],
														   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  con_msg_log.map(msg_log => msgLog.upgrade_replica(cluster_detail, msg_log))
	
	def new_replica(con_msg_log: CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
                 (implicit msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
                           msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
                           msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  con_msg_log.map(msg_log => msgLog.new_replica(msg_log))
}