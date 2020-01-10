package pure_ops
package implementation

import scala.collection.immutable._

import model.Model._

final case object BoundedCounter extends PureOpsCommCRDT {
  val init_data: HashMap[UNODE_ID, Long] = HashMap.empty[UNODE_ID, Long]
	
  def valid_ops(crdt_ops: CRDTOps): 
  Boolean = crdt_ops match {
    case BCI(_) => true
    case BCD(_) => true
    case BCX(_) => true
    case _      => false
  }
	
  def update_data(crdt_data: Any,
	          crdt_ops: CRDTOps):
  Any = {
    val data = crdt_data.asInstanceOf[HashMap[UNODE_ID, Long]]
    crdt_ops match {
      case BCI((node_id: UNODE_ID, value: Long))                      => 
	get_and_set(data, node_id, value)
      case BCD((node_id: UNODE_ID, value: Long))                      => 
        get_and_set(data, node_id, -value)
      case BCX((fnode_id: UNODE_ID, tnode_id: UNODE_ID, value: Long)) => 
	get_and_set(get_and_set(data, fnode_id, -value), tnode_id, value)
      case _                                                          => 
	data
    }
  }  
	
  def get_and_set(data: HashMap[UNODE_ID, Long], node_id: UNODE_ID, value: Long):
  HashMap[UNODE_ID, Long] = data.updated(node_id, data.getOrElse(node_id, 0L)+value)  
}
