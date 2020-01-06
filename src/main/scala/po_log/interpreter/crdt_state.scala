package po_log
package interpreter

import model.Model._
import pure_ops._

object CRDTStateInstances {
  implicit val uCRDTState: CRDTState[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new CRDTState[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
		}
}
