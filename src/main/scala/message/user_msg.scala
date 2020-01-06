package message

import model.Model._

trait UserMSG[CRDT_TYPE, CRDT_ID, CRDT_OPS] {
	def make(crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID], crdt_ops: CRDT_OPS):
	USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS] = USER_MSG(crdt_instance, crdt_ops)
	
	def get_crdt_instance(user_msg: USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_INSTANCE[CRDT_TYPE, CRDT_ID] = user_msg.crdt_instance
	
	def get_crdt_ops(user_msg: USER_MSG[CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	CRDT_OPS = user_msg.crdt_ops
}