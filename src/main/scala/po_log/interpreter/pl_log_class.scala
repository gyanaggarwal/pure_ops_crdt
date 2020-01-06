package po_log
package interpreter

import model.Model._
import pure_ops._

object POLogClassInstances {
  implicit val uPOLogClass: POLogClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new POLogClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
			def init_data(crdt_type: PureOpsCRDT) = crdt_type.init_data
		}
}
