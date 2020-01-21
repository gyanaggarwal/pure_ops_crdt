package po_log
package interpreter

import model.Model._
import pure_ops._
import vector_clock._
import message._
import msg_data._

object POLogClassInstances {
  implicit val uPOLogClass: POLogClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new POLogClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
			def init_data(crdt_type: PureOpsCRDT) = crdt_type.init_data

			def update_comm_crdt(msg_ops: MSG_OPS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
					                 po_log_class: PO_LOG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
													(implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
			PO_LOG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
				set_crdt_data(get_crdt_type(po_log_class)
					.update_comm_crdt(get_crdt_data(po_log_class), msgOpr.get_crdt_ops(msg_ops)), po_log_class)
					
			def add_con_msg(msg_ops: MSG_OPS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
			                po_log_class: PO_LOG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
                      (implicit vectorClock: VectorClock[UNODE_ID],
																nodeVCLOCK: NodeVCLOCK[UNODE_ID],
																conMSGLog: CONMSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
																msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
																msgData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
																msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
																msgLog: MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
			PO_LOG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
			  set_con_msg_log(get_crdt_type(po_log_class).add_con_msg(msg_ops, get_con_msg_log(po_log_class)), po_log_class)

			def update_causal_stable(po_log_class: PO_LOG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
				                      (implicit vectorClock: VectorClock[UNODE_ID],
																        tcsb: TCSB[UNODE_ID, UCLUSTER_ID],
																				nodeVCLOCK: NodeVCLOCK[UNODE_ID],
																				conMSGLog: CONMSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
																				msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
																				msgData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
																				msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
																				msgLog: MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
			PO_LOG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = {
				val crdt_type = get_crdt_type(po_log_class)
				val (msg_log, csmsg_log, con_msg_log) = crdt_type.split_msg(tcsb.causal_stable(get_tcsb_class(po_log_class)),
			                                                               get_con_msg_log(po_log_class),
																																	   get_msg_log(po_log_class))
				set_crdt_data(crdt_type.update_causal_stable(get_crdt_data(po_log_class), csmsg_log),
			                set_msg_log(msg_log, set_con_msg_log(con_msg_log, po_log_class)))
			}
		}
}
