package pure_ops

import model.Model._
import message._
import msg_data._

trait PureOpsNonCommCRDT extends PureOpsCRDT {
  def update_comm_crdt(crdt_data: Any,
	                     crdt_ops: CRDTOps):
	Any = crdt_data
	
	def init_any: Any
	
	def update_any(value: Any,
	               crdt_ops: CRDTOps):Any
	
	def combine_any(value0: Any,
	                value1: Any):Any

	def process_msg_log(msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
							       (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
								               msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	Any =  
		msg_log.foldLeft(init_any){case (mi0, (_, msg_class)) => {
			val val_any = msgClass.get_msg_data(msg_class).foldLeft(init_any){case (mi1, (_, msg_ops)) => {
        update_any(mi1, msgOpr.get_crdt_ops(msg_ops))				
			}}
			combine_any(mi0, val_any)
	}}
}
