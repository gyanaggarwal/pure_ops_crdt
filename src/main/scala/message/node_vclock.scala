package message

import model.Model._
import vector_clock._

trait NodeVCLOCK[NODE_ID] {
	def make(node_id: NODE_ID, vclock: VCLOCK[NODE_ID]):
	NODE_VCLOCK[NODE_ID] = NODE_VCLOCK(node_id, vclock)
	
	def get_node_id(node_vclock: NODE_VCLOCK[NODE_ID]):
	NODE_ID = node_vclock.node_id
	
	def get_vclock(node_vclock: NODE_VCLOCK[NODE_ID]):
	VCLOCK[NODE_ID] = node_vclock.vclock
	
	def set_vclock(vclock: VCLOCK[NODE_ID], node_vclock: NODE_VCLOCK[NODE_ID]):
	NODE_VCLOCK[NODE_ID] = node_vclock.copy(vclock = vclock)
	
	def logical_clock(node_vclock: NODE_VCLOCK[NODE_ID])
	                 (implicit vectorClock: VectorClock[NODE_ID]):
	LOGICAL_CLOCK = vectorClock.logical_clock(get_node_id(node_vclock), get_vclock(node_vclock))
}