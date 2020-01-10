package vector_clock
package interpreter

import scala.collection.immutable._

import model.Model._

object VectorClockInstances {
  implicit val uVectorClock: VectorClock[UNODE_ID] =
    new VectorClock[UNODE_ID] {
      val empty: VCLOCK[UNODE_ID] = HashMap.empty[UNODE_ID, LOGICAL_CLOCK]
	
      def merge_vc(vc0: VCLOCK[UNODE_ID],
		   vc1: VCLOCK[UNODE_ID],
	           mf: (LOGICAL_CLOCK, LOGICAL_CLOCK) => LOGICAL_CLOCK): 
      VCLOCK[UNODE_ID] = {
	val vc01 = vc0.asInstanceOf[HashMap[UNODE_ID, LOGICAL_CLOCK]]
	val vc11 = vc1.asInstanceOf[HashMap[UNODE_ID, LOGICAL_CLOCK]]
        vc01.merged(vc11)({case ((k0x, v0x), (_, v1x)) => (k0x, mf(v0x, v1x))})
      }
    }
}
