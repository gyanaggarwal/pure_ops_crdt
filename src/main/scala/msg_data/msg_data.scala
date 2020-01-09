package msg_data

import model.Model._
import vector_clock._
import cluster_config._
import message._
import util._

trait MSGData[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def empty: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]
	
	def add_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	            msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
						 (implicit vectorClock: VectorClock[NODE_ID],
							         nodeVCLOCK: NodeVCLOCK[NODE_ID],
											 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  msg_data.updated(msgOpr.logical_clock(msg_ops), msg_ops)
		
	def check_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		            msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							 (implicit vectorClock: VectorClock[NODE_ID],
								         nodeVCLOCK: NodeVCLOCK[NODE_ID],
												 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CHECKMSG = if (msg_data.contains(msgOpr.logical_clock(msg_ops))) DUPMSG else OOOMSG 

	def take_msg(msg_ops: MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
		           msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
							(implicit vectorClock: VectorClock[NODE_ID],
								        nodeVCLOCK: NodeVCLOCK[NODE_ID],
												msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	(MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
	 Option[MSG_OPS[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]]) = {
		val nlc: LOGICAL_CLOCK = vectorClock.next_clock(msgOpr.logical_clock(msg_ops))
		GenUtil.take_map(nlc, msg_data)
	}

 	def split_msg(csvc: VCLOCK[NODE_ID],
 		            msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
 							 (implicit vectorClock: VectorClock[NODE_ID],
 								         nodeVCLOCK: NodeVCLOCK[NODE_ID],
 												 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
 	(MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
 	 MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]) = {
 	  val csmsg_data = msg_data.filter{case (lc, msg_ops) => {
 	  	val mvc = msgOpr.get_vclock(msg_ops)
			vectorClock.comp_vc(mvc, csvc) match {
				case LTVC => true
				case EQVC => true
				case _    => false
			}
 	  }}
		(msg_data -- csmsg_data.keys, csmsg_data)
 	}

  def undeliv_msg(logical_clock: LOGICAL_CLOCK,
	                msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_LIST[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  msg_data.filterKeys(_ > logical_clock).values.toList
	 
  def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                    msg_data: MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
										 (implicit vectorClock: VectorClock[NODE_ID],
											         clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID], 
										           nodeVCLOCK: NodeVCLOCK[NODE_ID],
															 msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	MSG_DATA[NODE_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] = 
	  msg_data.mapValues(msg_ops => msgOpr.asMSG_OPS(msgOpr.upgrade_replica(cluster_detail, msg_ops)))
 }