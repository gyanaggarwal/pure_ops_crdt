package po_log
package interpreter

import model.Model._
import pure_ops._
import message._
import msg_data._

object POLogClassInstances {
  implicit val uPOLogClass: POLogClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new POLogClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
			def init_data(crdt_type: PureOpsCRDT) = crdt_type.init_data
			
			def causal_stable(msg_log: MSG_LOG[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
			                  po_log_class: PO_LOG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
											 (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
															   msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
			PO_LOG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
		    set_crdt_data(get_crdt_type(po_log_class)
				  .update_causal_stable(get_crdt_data(po_log_class), msg_log), po_log_class)
					
			def update_comm_crdt(msg_ops: MSG_OPS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
					                 po_log_class: PO_LOG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
													(implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
			PO_LOG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
				set_crdt_data(get_crdt_type(po_log_class)
					.update_comm_crdt(get_crdt_data(po_log_class), msgOpr.get_crdt_ops(msg_ops)), po_log_class)
		}
}
