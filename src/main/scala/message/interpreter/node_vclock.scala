package message
package interpreter

import model.Model._

object NodeVCLOCKInstances {
  implicit val uNodeVCLOCK: NodeVCLOCK[UNODE_ID] =
    new NodeVCLOCK[UNODE_ID] {
		}
}