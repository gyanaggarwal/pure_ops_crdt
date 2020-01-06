package pure_ops
package implementation

final case object GSet extends PureOpsCommCRDT {
  val init_data: Set[Any] = Set.empty[Any]
	
	def valid_ops(crdt_ops: CRDTOps): 
	Boolean = crdt_ops match {
		case ADD(_) => true
		case _      => false
	}
	
	def update_data(crdt_data: Any,
	                crdt_ops: CRDTOps):
	Any = crdt_ops match {
		case ADD(args) => crdt_data.asInstanceOf[Set[Any]]+args
		case _         => crdt_data
	}  
	
  override def eval_data(crdt_ops: CRDTOps,
	                       crdt_data: Any):
	Any = eval_set(crdt_ops, crdt_data.asInstanceOf[Set[Any]])	
}
