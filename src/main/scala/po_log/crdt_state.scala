package po_log

import model.Model._
import cluster_config._
import vector_clock._
import message._
import msg_data._
import util._

trait CRDTState[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
  def make(node_id: NODE_ID,
	         cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]):
	CRDT_STATE_NEW[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  CRDT_STATE_NEW(node_id, cluster_detail)

	def make(node_id: NODE_ID,
		       cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
				   po_log: PO_LOG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
		CRDT_STATE_DATA(node_id, cluster_detail, po_log)
		
	def empty(node_id: NODE_ID,
		        cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID])
					 (implicit poLog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  make(node_id, cluster_detail, poLog.empty)
	
	def asCRDT_STATE_DATA(crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  crdt_state.asInstanceOf[CRDT_STATE_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]
		
	def asCRDT_STATE_NEW(crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE_NEW[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  crdt_state.asInstanceOf[CRDT_STATE_NEW[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]

	def get_node_id(crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	NODE_ID = crdt_state match {
		case _: TYPE_CRDT_STATE_DATA => asCRDT_STATE_DATA(crdt_state).node_id
		case _: TYPE_CRDT_STATE_NEW  => asCRDT_STATE_NEW(crdt_state).node_id 
	}

	def get_cluster_detail(crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CLUSTER_DETAIL[NODE_ID, CLUSTER_ID] = crdt_state match {
		case _: TYPE_CRDT_STATE_DATA => asCRDT_STATE_DATA(crdt_state).cluster_detail
		case _: TYPE_CRDT_STATE_NEW  => asCRDT_STATE_NEW(crdt_state).cluster_detail 
	}
	
	def get_po_log(crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	Option[PO_LOG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]] = crdt_state match {
		case _: TYPE_CRDT_STATE_DATA => Some(asCRDT_STATE_DATA(crdt_state).po_log)
		case _: TYPE_CRDT_STATE_NEW  => None
	}
	
	def set_cluster_detail(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                       crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = crdt_state match {
		case _: TYPE_CRDT_STATE_DATA => asCRDT_STATE_DATA(crdt_state).copy(cluster_detail = cluster_detail)
		case _: TYPE_CRDT_STATE_NEW  => asCRDT_STATE_NEW(crdt_state).copy(cluster_detail = cluster_detail)
	}
	
	def set_po_log(po_log: PO_LOG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	               crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = crdt_state match {
		case _: TYPE_CRDT_STATE_DATA => asCRDT_STATE_DATA(crdt_state).copy(po_log = po_log)
		case _: TYPE_CRDT_STATE_NEW  => crdt_state
	}

  def add_po_log(po_log: PO_LOG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	               crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  make(get_node_id(crdt_state), get_cluster_detail(crdt_state), po_log)

	def get_po_log_class(crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
		                   po_log: PO_LOG[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
					             crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
					             anyId: AnyId[NODE_ID])
					            (implicit crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
												        clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
									              vectorClock: VectorClock[NODE_ID],
									              tcsb: TCSB[NODE_ID, CLUSTER_ID],
									              msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									              pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															  polog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] =
	  polog.get(get_node_id(crdt_state), 
		          clusterConfig.get_node_list(get_cluster_detail(crdt_state)),
						  crdt_instance,
						  po_log,
						  anyId)
		 
  def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
				              crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										 (implicit vectorClock: VectorClock[NODE_ID],
													     tcsb: TCSB[NODE_ID, CLUSTER_ID],
															 clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
															 nodeVCLOCK: NodeVCLOCK[NODE_ID],
															 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															 msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															 pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   polog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = crdt_state match {
		case _: TYPE_CRDT_STATE_DATA => 
		get_po_log(crdt_state).fold(crdt_state)(po_log => {
			set_po_log(polog.upgrade_replica(cluster_detail, po_log), crdt_state)
		})
		case _: TYPE_CRDT_STATE_NEW => crdt_state
	}

	def new_replica(fcrdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
			            crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
								  anyId: AnyId[CLUSTER_ID])
								 (implicit clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
									         tcsb: TCSB[NODE_ID, CLUSTER_ID],
										       msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													 msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												   pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												   polog: POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = (fcrdt_state, crdt_state) match {
		case (_: TYPE_CRDT_STATE_DATA, _: TYPE_CRDT_STATE_NEW) => {
			val fcluster_detail = get_cluster_detail(fcrdt_state)
			val cluster_detail = get_cluster_detail(crdt_state) 
		  (clusterConfig.comp_cc(fcluster_detail, cluster_detail, anyId),
			 clusterConfig.valid_node_list(fcluster_detail, cluster_detail)) match {
		  	case (EQCC, true) => {
					get_po_log(fcrdt_state).fold(crdt_state)(po_log => {
						add_po_log(polog.new_replica(get_node_id(crdt_state), get_node_id(fcrdt_state), po_log),
					             crdt_state)
					})
		  	}
				case (_, _)       => crdt_state
		  }}
		case (_, _)                                            => crdt_state
	}
}