package middleware

import scala.collection.immutable._

import model.Model._
import pure_ops._
import cluster_config._
import vector_clock._
import message._
import msg_data._
import po_log._
import util._

trait MSGSndRcv[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def empty_undeliv_msg: UNDELIV_MSG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	
	def empty_snd_msg_data: SND_MSG_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	
	def get_cluster_detail(undeliv_msg_class: UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CLUSTER_DETAIL[NODE_ID, CLUSTER_ID] = undeliv_msg_class.cluster_detail
	
	def get_undeliv_msg(undeliv_msg_class: UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	UNDELIV_MSG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = undeliv_msg_class.undeliv_msg
	
	def set_undeliv_msg(undeliv_msg: UNDELIV_MSG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                    undeliv_msg_class: UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = undeliv_msg_class.copy(undeliv_msg = undeliv_msg)
	
	def add_undeliv_msg(crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
	                    msg_list: MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  undeliv_msg_class: UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  set_undeliv_msg(get_undeliv_msg(undeliv_msg_class).updated(crdt_instance, msg_list), undeliv_msg_class)
											
	def make_undeliv_msg_class(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                           undeliv_msg: UNDELIV_MSG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = UNDELIV_MSG_CLASS(cluster_detail, undeliv_msg)
		
	def undeliv_msg_class(node_id: NODE_ID,
		                    cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                      snd_msg_data: SND_MSG_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	UNDELIV_MSG_CLASS[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  snd_msg_data.getOrElse(node_id, make_undeliv_msg_class(cluster_detail, empty_undeliv_msg))
								
	def snd_msg_data(tnode_list: List[NODE_ID],
		               crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                 anyId: AnyId[NODE_ID])
	 								(implicit vectorClock: VectorClock[NODE_ID],
	 											    nodeVCLOCK: NodeVCLOCK[NODE_ID],
														tcsb: TCSB[NODE_ID, CLUSTER_ID],
	 													msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 													msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 													msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										        crdtState: CRDTState[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	SND_MSG_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val smd = empty_snd_msg_data
		crdtState.get_po_log(crdt_state).fold(smd)(po_log => {
			val node_id = crdtState.get_node_id(crdt_state)
		  make_undeliv_msg(node_id, 
			                 GenUtil.remove_from_list(crdtState.get_node_id(crdt_state), tnode_list, anyId),
		                   crdtState.get_cluster_detail(crdt_state), 
										   po_log, 
										   smd, 
										   anyId)}
		)
	}
	
	def make_undeliv_msg(node_id: NODE_ID,
		                   tnode_list: List[NODE_ID],
	                     cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
										   po_log: PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										   smd: SND_MSG_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										   anyId: AnyId[NODE_ID])
			 								 (implicit vectorClock: VectorClock[NODE_ID],
			 											     nodeVCLOCK: NodeVCLOCK[NODE_ID],
																 tcsb: TCSB[NODE_ID, CLUSTER_ID],
			 													 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
			 													 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
			 													 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																 pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	SND_MSG_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  po_log.foldLeft(smd){case (smd0, (crdt_instance, po_log_class)) => 
	    make_msg_data(node_id, 
				            tnode_list, 
									  cluster_detail, 
									  crdt_instance, 
									  po_log_class, 
									  smd0, 
									  anyId)
	}
	
	def make_msg_data(node_id: NODE_ID,
		                tnode_list: List[NODE_ID],
	                  cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
										crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
										po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										smd: SND_MSG_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										anyId: AnyId[NODE_ID])
	 								 (implicit vectorClock: VectorClock[NODE_ID],
	 											     nodeVCLOCK: NodeVCLOCK[NODE_ID],
														 tcsb: TCSB[NODE_ID, CLUSTER_ID],
	 													 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 													 msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 													 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														 pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	SND_MSG_DATA[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val tcsb_class = pologClass.get_tcsb_class(po_log_class)
		val msg_log = pologClass.get_msg_log(po_log_class)
		val pvclock = tcsb.get_peer_vclock(tcsb_class)
		val msg_vclock = msgOpr.make_msg_vclock(nodeVCLOCK.make(node_id, tcsb.get_node_vclock(tcsb_class)), 
		                                        crdt_instance)
		tnode_list.foldLeft(smd)((smd0, tnode_id) => {
			pvclock.get(tnode_id)
			  .fold(smd0)(tvclock => {
					smd0.updated(tnode_id, 
											 add_undeliv_msg(crdt_instance, 
																			 get_undeliv_msg(node_id, tnode_id, tvclock, msg_vclock, msg_log, anyId), 
																			 undeliv_msg_class(tnode_id, cluster_detail, smd0)))
																								
			})
		})
	}
											
	def get_undeliv_msg(node_id: NODE_ID,
	                    tnode_id: NODE_ID,
										  tvclock: VCLOCK[NODE_ID],
										  msg_vclock: MESSAGE[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  msg_log: MSG_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
										  anyId: AnyId[NODE_ID])
										 (implicit vectorClock: VectorClock[NODE_ID],
											         nodeVCLOCK: NodeVCLOCK[NODE_ID],
														   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
														   msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
    tvclock.filterKeys(!anyId.eqv(_, tnode_id))
		  .foldLeft(msgOpr.empty_msg_list){case (ml0, (snode_id, slogical_clock)) => {
		  	val msg_list0 = msg_log.get(snode_id)
				                  .fold(msgOpr.empty_msg_list)(msg_class => msgClass.undeliv_msg(slogical_clock, msg_class))
				ml0 ++ (if (anyId.eqv(node_id, snode_id)) add_msg_vclock(msg_vclock, msg_list0) else msg_list0)
		}}
	}
															 
	def add_msg_vclock(msg_vclock: MESSAGE[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	                   msg_list: MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										(implicit vectorClock: VectorClock[NODE_ID],
											        nodeVCLOCK: NodeVCLOCK[NODE_ID],
										          msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = {
		val rmsg_list = msg_list.reverse
		rmsg_list.headOption
		  .fold(msg_vclock :: msg_list)(msg_ops => {
		  	vectorClock.equal(msgOpr.get_vclock(msg_vclock), msgOpr.get_vclock(msg_ops)) match {
		  		case true  => msg_list
					case false => (msg_vclock :: rmsg_list).reverse
		  	}
		  })
	}
	
	def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                    undeliv_msg: UNDELIV_MSG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										 (implicit vectorClock: VectorClock[NODE_ID],
											         clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
										           nodeVCLOCK: NodeVCLOCK[NODE_ID],
														   msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	UNDELIV_MSG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  undeliv_msg.mapValues(msg_list => msgOpr.upgrade_replica(cluster_detail, msg_list))
}

final case object MSGSndRcv extends MSGSndRcv[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {	
	val empty_undeliv_msg: 
	  UNDELIV_MSG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
		HashMap.empty[CRDT_INSTANCE[PureOpsCRDT, UCRDT_ID], 
		              MSG_LIST[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]
	
	val empty_snd_msg_data: 
	  SND_MSG_DATA[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
		HashMap.empty[UNODE_ID, 
		              UNDELIV_MSG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]
}