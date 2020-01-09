package po_log
package interpreter

import scala.collection.immutable._

import model.Model._
import pure_ops._

object POLogInstances {
  implicit val uPOLog: POLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new POLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
			val empty: PO_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
			  HashMap.empty[CRDT_INSTANCE[PureOpsCRDT, UCRDT_ID], 
				              PO_LOG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]
		}
}