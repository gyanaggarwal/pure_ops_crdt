package pure_ops
package implementation

final case object TPSet extends PureOpsCommCRDT {
  val init_data: ARSet = ARSet()
	
	def valid_ops(crdt_ops: CRDTOps): 
	Boolean = crdt_ops match {
		case ADD(_) => true
		case RMV(_) => true
		case _      => false
	}
	
	def update_data(crdt_data: Any,
	                crdt_ops: CRDTOps):
	Any = {
		val data = crdt_data.asInstanceOf[ARSet]
		crdt_ops match {
		  case ADD(args) => if (data.rset.contains(args)) data else data.copy(aset = data.aset+args)
			case RMV(args) => data.copy(aset = data.aset-args, rset = data.rset+args)
		  case _         => crdt_data
		}
	}  
	
  override def eval_data(crdt_ops: CRDTOps,
	                       crdt_data: Any):
	Any = eval_set(crdt_ops, crdt_data.asInstanceOf[ARSet].aset)	
}
