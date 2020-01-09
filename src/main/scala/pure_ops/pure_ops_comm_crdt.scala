package pure_ops

import model.Model._
import message._
import msg_data._

trait PureOpsCommCRDT extends PureOpsCRDT {
	def update_data(crdt_data: Any,
	                crdt_ops: CRDTOps): Any

  def update_comm_crdt(crdt_data: Any,
	                     crdt_ops: CRDTOps):
	Any = update_data(crdt_data, crdt_ops)	

  def combine_msg_log_data(msg_log_data: Any,
	                         crdt_data: Any): 
	Any = crdt_data
	
	def process_msg_log(msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
							       (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
								               msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	Any = ()  
	
}
