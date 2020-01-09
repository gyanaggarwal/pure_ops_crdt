package msg_data
package interpreter

import scala.collection.immutable._

import model.Model._
import pure_ops._

object MSGDataInstances {
  implicit val uMSGData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
			val empty: MSG_DATA[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
			  SortedMap.empty[LOGICAL_CLOCK, MSG_OPS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]
		}
}