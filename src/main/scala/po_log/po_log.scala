package po_log

import model.Model._
import cluster_config._
import vector_clock._
import message._
import msg_data._
import util._

trait POLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def empty: PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	
	def add(crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
	        po_log_class: PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
				  po_log: PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = po_log.updated(crdt_instance, po_log_class)
	
	def create(node_id: NODE_ID,
	           node_list: List[NODE_ID],
				     crdt_instance_list: List[CRDT_INSTANCE[CRDT_TYPE, CRDT_ID]],
				     anyId: AnyId[NODE_ID])
				    (implicit crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
								      vectorClock: VectorClock[NODE_ID],
								      tcsb: TCSB[NODE_ID, CLUSTER_ID],
											conMsgList: CONMsgList[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
								      msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
								      pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  crdt_instance_list.foldLeft(empty)((po_log, crdt_instance) => {
	    val po_log_class = pologClass.create(node_id, node_list, crdtInstance.get_crdt_type(crdt_instance), anyId)
			po_log.updated(crdt_instance, po_log_class)  	
	})
		
	def get(node_id: NODE_ID,
	        node_list: List[NODE_ID],
				  crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
				  po_log: PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
				  anyId: AnyId[NODE_ID])
				 (implicit crdtInstance: CRDTInstance[CRDT_TYPE, CRDT_ID],
								   vectorClock: VectorClock[NODE_ID],
								   tcsb: TCSB[NODE_ID, CLUSTER_ID],
									 conMsgList: CONMsgList[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
								   msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
								   pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG_CLASS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  po_log.get(crdt_instance).
		  fold(pologClass.create(node_id, node_list, crdtInstance.get_crdt_type(crdt_instance), anyId))(plc => plc)
			
  def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
			                po_log: PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
											(implicit vectorClock: VectorClock[NODE_ID],
												        tcsb: TCSB[NODE_ID, CLUSTER_ID],
																clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID],
																nodeVCLOCK: NodeVCLOCK[NODE_ID],
																conMsgList: CONMsgList[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
																msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
															  pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  po_log.mapValues(plc => pologClass.upgrade_replica(cluster_detail, plc))

	def new_replica(rnode_id: NODE_ID,
			            fnode_id: NODE_ID,
									po_log: PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
								 (implicit tcsb: TCSB[NODE_ID, CLUSTER_ID],
										       msgData: MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													 msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
													 msgLog: MSGLog[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
												   pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	PO_LOG[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  po_log.mapValues(plc => pologClass.new_replica(rnode_id, fnode_id, plc))
}