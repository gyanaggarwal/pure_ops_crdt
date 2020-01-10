package vector_clock
package interpreter

import scala.collection.immutable._

import model.Model._

object TCSBInstances {
  implicit val uTCSB: TCSB[UNODE_ID, UCLUSTER_ID] = 
    new TCSB[UNODE_ID, UCLUSTER_ID] {
      val empty: PEER_VCLOCK[UNODE_ID] = HashMap.empty[UNODE_ID, VCLOCK[UNODE_ID]]
    }
}
