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
				
			def merge(msg_log0: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
				        msg_log1: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
							 (implicit msgData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
							           msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
			MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps] = {
				val ml0 = msg_log0.asInstanceOf[HashMap[UNODE_ID, MSG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]]
				val ml1 = msg_log1.asInstanceOf[HashMap[UNODE_ID, MSG_CLASS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]]]
				ml0.merged(ml1){case ((k0, mc0), (_, mc1)) => (k0, msgClass.merge(mc0, mc1))}
			}
		}
}