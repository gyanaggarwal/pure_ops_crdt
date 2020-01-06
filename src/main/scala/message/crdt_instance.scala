package message

import model.Model._

trait CRDTInstance[CRDT_TYPE, CRDT_ID] {
	def make(crdt_type: CRDT_TYPE, crdt_id: CRDT_ID):
	CRDT_INSTANCE[CRDT_TYPE, CRDT_ID] = CRDT_INSTANCE(crdt_type, crdt_id)
	
	def get_crdt_type(crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID]):
	CRDT_TYPE = crdt_instance.crdt_type
}