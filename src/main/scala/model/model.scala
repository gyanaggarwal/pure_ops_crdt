package model

object Model {
/* following types can be changes */
	
	type UNODE_ID                  = Int
	type UCRDT_ID                  = Int
	type UCLUSTER_ID               = Int

/* following types should not be changed */
	  
	type LOGICAL_CLOCK             = Long
	type CLUSTER_VER_NUM           = Int
	type CONC_MSG_KEY              = Long
	
//HashMap
	type VCLOCK[NODE_ID] = Map[NODE_ID, LOGICAL_CLOCK] 

//HashMap of peer nodes vector_clock
	type PEER_VCLOCK[NODE_ID] = Map[NODE_ID, VCLOCK[NODE_ID]] 

	final case class TCSB_CLASS[NODE_ID]
	                 (node_vclock: VCLOCK[NODE_ID],
	                  peer_vclock: PEER_VCLOCK[NODE_ID])

  final case class CRDT_INSTANCE[CRDT_TYPE, CRDT_ID]
	                 (crdt_type: CRDT_TYPE,
									  crdt_id:   CRDT_ID)
										
	final case class NODE_VCLOCK[NODE_ID]
	                 (node_id: NODE_ID,
									  vclock:  VCLOCK[NODE_ID])
										
  sealed trait CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]
	final case class ClusterDetailADD[NODE_ID, CLUSTER_ID]
								 	 (cluster_id: CLUSTER_ID,
								 	  ver_num:    CLUSTER_VER_NUM = 0,
										node_id:    NODE_ID,
								 	  node_list:  List[NODE_ID]) 
								 	 extends CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]
	final case class ClusterDetailRMV[NODE_ID, CLUSTER_ID]
									 (cluster_id: CLUSTER_ID,
										ver_num:    CLUSTER_VER_NUM = 0,
										node_id:    NODE_ID,
										node_list:  List[NODE_ID]) 
									 extends CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]

 	final case class USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS]
	                 (crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
									  crdt_ops:      CRDT_OPS)
										
	sealed trait MESSAGE[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	final case class MSG_VCLOCK[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	                 (node_vclock:    NODE_VCLOCK[NODE_ID],
									  crdt_instance:  CRDT_INSTANCE[CRDT_TYPE, CRDT_ID])
									 extends MESSAGE[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] 																											
	final case class MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
								 	 (node_vclock:    NODE_VCLOCK[NODE_ID],
								 		crdt_instance:  CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
									  crdt_ops:       CRDT_OPS)
								 	 extends MESSAGE[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] 																											

	type TYPE_CLUSTER_DETAIL_ADD = ClusterDetailADD[_, _]
	type TYPE_CLUSTER_DETAIL_RMV = ClusterDetailRMV[_, _]

  type TYPE_MSG_VCLOCK         = MSG_VCLOCK[_, _, _, _]
	type TYPE_MSG_OPS            = MSG_OPS[_, _, _, _]	

  type MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
    List[MESSAGE[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]
		
//SortedMap
  type MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
		Map[LOGICAL_CLOCK, MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]

	final case class MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	                 (msg_data:         MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									  pending_msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])

//HashMap 
  type MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
    Map[NODE_ID, MSG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]
		
	type CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  List[MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]
		
	final case class PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	                 (tcsb_class:   TCSB_CLASS[NODE_ID],
										con_msg_log:  CON_MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									  msg_log:      MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									  crdt_type:    CRDT_TYPE,
									  crdt_data:    Any)
	
//HashMap										
	type PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  Map[CRDT_INSTANCE[CRDT_TYPE, CRDT_ID], PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]
		
	sealed trait CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	final case class CRDT_STATE_NEW[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	                 (node_id:        NODE_ID,
									  cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID])
									 extends CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	final case class CRDT_STATE_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	                 (node_id:        NODE_ID,
									  cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
									  po_log:         PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
									 extends CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
									 
	type TYPE_CRDT_STATE_NEW  = CRDT_STATE_NEW[_, _, _, _, _]
	type TYPE_CRDT_STATE_DATA = CRDT_STATE_DATA[_, _, _, _, _]
	 
//HashMap for list of messages for a CRDT that have not been delivered to a peer node
  type UNDELIV_MSG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
		Map[CRDT_INSTANCE[CRDT_TYPE, CRDT_ID], MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]

  final case class UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	                 (cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
									  undeliv_msg:    UNDELIV_MSG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])	
	
//HashMap for all the peer nodes for undelivered messages
	type SND_MSG_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
		Map[NODE_ID, UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]
	
}