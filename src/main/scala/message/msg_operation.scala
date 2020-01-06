package message

import model.Model._
import vector_clock._
import cluster_config._

trait MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
  def empty_msg_list: MSG_LIST[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]

  def make_msg_vclock(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                    node_vclock: NODE_VCLOCK[NODE_ID],
										  crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID]):
	MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  MSG_VCLOCK(cluster_detail, node_vclock, crdt_instance)

  def make_msg_ops(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                 node_vclock: NODE_VCLOCK[NODE_ID],
									 crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
								   crdt_ops: CRDT_OPS):
	MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  MSG_OPS(cluster_detail, node_vclock, crdt_instance, crdt_ops)

  def make_user_msg_ops(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                      node_vclock: NODE_VCLOCK[NODE_ID],
											  user_msg: USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS])
											 (implicit userMsg: UserMSG[CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  make_msg_ops(cluster_detail, node_vclock, userMsg.get_crdt_instance(user_msg), userMsg.get_crdt_ops(user_msg))
		
	def get_crdt_ops(msg_ops: MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_OPS = msg_ops.crdt_ops

  def asMSG_OPS(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  msg.asInstanceOf[MSG_OPS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]
			
  def asMSG_VCLOCK(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_VCLOCK[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  msg.asInstanceOf[MSG_VCLOCK[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]

	def get_cluster_detail(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CLUSTER_DETAIL[NODE_ID, CLUSTER_ID] = msg match {
		case _: TYPE_MSG_OPS    => asMSG_OPS(msg).cluster_detail
		case _: TYPE_MSG_VCLOCK => asMSG_VCLOCK(msg).cluster_detail
	}

	def get_node_vclock(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	NODE_VCLOCK[NODE_ID] = msg match {
		case _: TYPE_MSG_OPS    => asMSG_OPS(msg).node_vclock
		case _: TYPE_MSG_VCLOCK => asMSG_VCLOCK(msg).node_vclock
	}

	def get_crdt_instance(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_INSTANCE[CRDT_TYPE, CRDT_ID] = msg match {
		case _: TYPE_MSG_OPS    => asMSG_OPS(msg).crdt_instance
		case _: TYPE_MSG_VCLOCK => asMSG_VCLOCK(msg).crdt_instance
	}

	def get_crdt_type(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
	                 (implicit crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID]):
	CRDT_TYPE = crdtInstance.get_crdt_type(get_crdt_instance(msg))
	 
	def get_node_id(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
	               (implicit nodeVCLOCK: NodeVCLOCK[NODE_ID]):
	NODE_ID = nodeVCLOCK.get_node_id(get_node_vclock(msg))
	
	def get_vclock(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
	              (implicit nodeVCLOCK: NodeVCLOCK[NODE_ID]):
	VCLOCK[NODE_ID] = nodeVCLOCK.get_vclock(get_node_vclock(msg))
	 
	def set_vclock(vclock: VCLOCK[NODE_ID],
		             msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								 (implicit nodeVCLOCK: NodeVCLOCK[NODE_ID]):
	MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val nvc0 = get_node_vclock(msg)
		val nvc1 = nodeVCLOCK.set_vclock(vclock, nvc0)
		msg match {
			case _: TYPE_MSG_OPS    => asMSG_OPS(msg).copy(node_vclock = nvc1)
			case _: TYPE_MSG_VCLOCK => asMSG_VCLOCK(msg).copy(node_vclock = nvc1)
		}
	}
	
	def set_cluster_detail(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
		                     msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = msg match {
		case _: TYPE_MSG_OPS    => asMSG_OPS(msg).copy(cluster_detail = cluster_detail)
		case _: TYPE_MSG_VCLOCK => asMSG_VCLOCK(msg).copy(cluster_detail = cluster_detail)
	}
	
	def logical_clock(msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
	                 (implicit vectorClock: VectorClock[NODE_ID],
	                           nodeVCLOCK: NodeVCLOCK[NODE_ID]):
	LOGICAL_CLOCK = nodeVCLOCK.logical_clock(get_node_vclock(msg))
	
  def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
                      msg: MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
			               (implicit vectorClock: VectorClock[NODE_ID],
			                         clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
														   nodeVCLOCK: NodeVCLOCK[NODE_ID]):
	MESSAGE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val msg1 = cluster_detail match {
			case _: TYPE_CLUSTER_DETAIL_ADD => {
        val vclock = vectorClock.merge_max(clusterConfig.get_vclock(cluster_detail), get_vclock(msg))
				set_vclock(vclock, msg)
			} 
			case _: TYPE_CLUSTER_DETAIL_RMV => msg
		} 
		set_cluster_detail(cluster_detail, msg1)
  }

  def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
                      msg_list: MSG_LIST[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
			               (implicit vectorClock: VectorClock[NODE_ID],
			                         clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
														   nodeVCLOCK: NodeVCLOCK[NODE_ID]):
	MSG_LIST[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  msg_list.map(msg => upgrade_replica(cluster_detail, msg))
}