package msg_data
package interpreter

import scala.collection.immutable._

import model.Model._
import pure_ops._

object MSGLogInstances {
  implicit val uMSGLog: MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] =
    new MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] {
			val empty: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = 
			  HashMap.empty[UNODE_ID, MSG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]
		}
}