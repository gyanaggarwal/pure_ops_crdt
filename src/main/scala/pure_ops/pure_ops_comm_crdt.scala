package pure_ops

import model.Model._
import vector_clock._
import message._
import msg_data._

trait PureOpsCommCRDT extends PureOpsCRDT {
  def combine_msg_log_data(msg_log_data: Any,
	                         crdt_data: Any): 
	Any = crdt_data
	
	def process_msg_log(msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
							       (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
								               msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	Any = () 
	
	def add_con_msg(msg_ops: MSG_OPS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                con_msg_list: CON_MSG_LIST[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
								 (implicit vectorClock: VectorClock[UNODE_ID],
								           nodeVCLOCK: NodeVCLOCK[UNODE_ID],
												   msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   conMsgList: CONMsgList[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	CON_MSG_LIST[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = con_msg_list

	def split_msg(csvclock: VCLOCK[UNODE_ID],
	              con_msg_list: CON_MSG_LIST[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
							  msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
							 (implicit vectorClock: VectorClock[UNODE_ID],
							           nodeVCLOCK: NodeVCLOCK[UNODE_ID],
											   msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   msgData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   msgLog: MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   conMsgList: CONMsgList[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	(MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	 MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
   CON_MSG_LIST[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = {
		 val (ml, csmsg_log) = msgLog.split_msg(csvclock, msg_log)
		 (ml, csmsg_log, con_msg_list)
  }
}
