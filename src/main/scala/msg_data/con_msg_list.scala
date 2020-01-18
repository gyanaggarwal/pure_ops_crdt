package msg_data

import model.Model._
import cluster_config._
import vector_clock._
import message._

trait CONMsgList[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def empty: CON_MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	
	def msg_list(msg: MESSAGE[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
	            (implicit msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = msg :: msgOpr.empty_msg_list
	
	def check_con(vclock: VCLOCK[NODE_ID],
	              msg_list: MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							 (implicit vectorClock: VectorClock[NODE_ID],
										     nodeVCLOCK: NodeVCLOCK[NODE_ID],
												 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	COMPVC = 
	  msg_list.foldLeft(UNDVC: COMPVC){
			case (CONVC, _) => CONVC
			case (_, msg)   => vectorClock.comp_vc(vclock, msgOpr.get_vclock(msg)) 
	}

	def check_stable(vclock: VCLOCK[NODE_ID],
	                 msg_list: MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							    (implicit vectorClock: VectorClock[NODE_ID],
										        nodeVCLOCK: NodeVCLOCK[NODE_ID],
												    msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	COMPVC = 
	  msg_list.foldLeft(UNDVC: COMPVC){
			case (CONVC, _) => CONVC
			case (GTVC, _)  => GTVC
			case (_, msg)   => vectorClock.comp_vc(vclock, msgOpr.get_vclock(msg)) 
	}
	
	def add_msg(msg: MESSAGE[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	            con_msg_list: CON_MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						 (implicit vectorClock: VectorClock[NODE_ID],
						           nodeVCLOCK: NodeVCLOCK[NODE_ID],
										   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CON_MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val mvclock = msgOpr.get_vclock(msg)
		val (cml, ml) = con_msg_list.foldLeft((empty, msg_list(msg))){
			case ((cml0, ml0), mlx0) => check_con(mvclock, mlx0) match {
				case CONVC => (cml0, ml0 ++ mlx0)
				case _     => (mlx0 :: cml0, ml0)
			}
		}
		ml :: cml
	}
	
	def split_msg(csvclock: VCLOCK[NODE_ID],
	              con_msg_list: CON_MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						   (implicit vectorClock: VectorClock[NODE_ID],
						             nodeVCLOCK: NodeVCLOCK[NODE_ID],
										     msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
											   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(CON_MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]) = 
		con_msg_list.foldLeft((empty, msgLog.empty)){
			case ((cml0, ml0), mlx0) => check_stable(csvclock, mlx0) match {
				case GTVC => (cml0, msgLog.add_msg(mlx0, ml0))
				case EQVC => (cml0, msgLog.add_msg(mlx0, ml0))
				case _    => (mlx0 :: cml0, ml0)
			}
	}
	
	def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                    con_msg_list: CON_MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										 (implicit vectorClock: VectorClock[NODE_ID],
										           clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
														   nodeVCLOCK: NodeVCLOCK[NODE_ID],
														   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CON_MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  con_msg_list.map(msg_list => msgOpr.upgrade_replica(cluster_detail, msg_list))
}