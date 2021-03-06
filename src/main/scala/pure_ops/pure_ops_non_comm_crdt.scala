package pure_ops

import model.Model._
import vector_clock._
import message._
import msg_data._

trait PureOpsNonCommCRDT extends PureOpsCRDT {
  def update_comm_crdt(crdt_data: Any,
	                     crdt_ops: CRDTOps):
	Any = crdt_data
	
	def init_any: Any
	
  def isConcurrent(crdt_ops0: CRDTOps, 
		               crdt_ops1: CRDTOps): Boolean
	
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
	
	def add_con_msg(msg_ops: MSG_OPS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                con_msg_log: CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
								 (implicit vectorClock: VectorClock[UNODE_ID],
								           nodeVCLOCK: NodeVCLOCK[UNODE_ID],
												   conMSGLog: CONMSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   msgData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   msgLog: MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
	  conMSGLog.add_msg(msg_ops, con_msg_log, isConcurrent)
		
	def split_msg(csvc: VCLOCK[UNODE_ID],
	              con_msg_log: CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
							  msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
 							 (implicit vectorClock: VectorClock[UNODE_ID],
 							           nodeVCLOCK: NodeVCLOCK[UNODE_ID],
 											   conMSGLog: CONMSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
 											   msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
 											   msgData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
 											   msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
 											   msgLog: MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	(MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	 MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
   CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = {
		 val (cml, csmsg_log) = conMSGLog.split_msg(csvc, con_msg_log)
		 (msgLog.remove_msg(csmsg_log, msg_log), csmsg_log, cml)
  }
}
