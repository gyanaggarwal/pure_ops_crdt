package middleware

import model.Model._
import pure_ops._
import message._
import msg_data._
import po_log._

trait QueryFromUser[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS] {
  def eval(crdt_instance: CRDT_INSTANCE[CRDT_TYPE, CRDT_ID],
	         crdt_ops: CRDT_OPS,
				   crdt_state: CRDT_STATE[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS])
					(implicit msgOpr: MSGOperation[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
					          msgClass: MSGClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									  pologClass: POLogClass[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									  crdtState: CRDTState[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS],
									  updateCRDT: UpdateCRDT[NODE_ID, CLUSTER_ID, CRDT_TYPE, CRDT_ID, CRDT_OPS]):
	Option[Any] = 
	  crdtState.get_po_log(crdt_state)
	    .fold(None: Option[Any])(po_log => po_log.get(crdt_instance)
	    .fold(None: Option[Any])(po_log_class => Some(updateCRDT.query(pologClass.get_crdt_type(po_log_class),
																													           crdt_ops,
																													           pologClass.get_crdt_data(po_log_class),
																													           pologClass.get_msg_log(po_log_class)))))
}

final case object QueryFromUser extends QueryFromUser[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
}