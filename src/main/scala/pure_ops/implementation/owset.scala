package pure_ops
package implementation

import scala.collection.immutable._

trait OWSet extends PureOpsNonCommCRDT {
	val init_data: HashSet[Any] = HashSet.empty[Any]
	
	val init_any = ARSet()
	
	def valid_ops(crdt_ops: CRDTOps): 
	Boolean = crdt_ops match {
		case ADD(_) => true
		case RMV(_) => true
		case _      => false
	}
	
  def isConcurrent(crdt_ops0: CRDTOps, crdt_ops1: CRDTOps):
	Boolean = (crdt_ops0, crdt_ops1) match {
		case (ADD(args0), RMV(args1)) => (((init_data+args0)-args1).size == 0)
		case (RMV(args1), ADD(args0)) => (((init_data+args0)-args1).size == 0)
		case _                        => false
	}
		
	def combine_ARSet(ar_set0: ARSet,
	                  ar_set1: ARSet):ARSet
	
	def update_any(value: Any,
	               crdt_ops: CRDTOps):
	Any = {
		val ar_set = value.asInstanceOf[ARSet]
		crdt_ops match {
			case ADD(args) => ARSet(ar_set.aset+args, ar_set.rset-args)
			case RMV(args) => ARSet(ar_set.aset-args, ar_set.rset+args)
			case _         => ar_set
		}
	} 
	
	def combine_any(value0: Any,
								  value1: Any):
	Any = combine_ARSet(value0.asInstanceOf[ARSet], value1.asInstanceOf[ARSet])
	
  def combine_msg_log_data(msg_log_data: Any,
	                         crdt_data: Any): 
	Any = crdt_data.asInstanceOf[HashSet[Any]] ++ msg_log_data.asInstanceOf[ARSet].aset
	
  override def eval_data(crdt_ops: CRDTOps,
	                       crdt_data: Any):
	Any = eval_set(crdt_ops, crdt_data.asInstanceOf[HashSet[Any]])
}