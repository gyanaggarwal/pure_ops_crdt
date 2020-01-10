package pure_ops
package implementation

final case object GCounter extends PureOpsCommCRDT {
  val init_data: Long = 0L
	
	def valid_ops(crdt_ops: CRDTOps): 
	Boolean = crdt_ops match {
		case INC(_) => true
		case _      => false
	}
	
	def update_comm_crdt(crdt_data: Any,
	                     crdt_ops: CRDTOps):
	Any = crdt_ops match {
		case INC(args) => crdt_data.asInstanceOf[Long]+args
		case _         => crdt_data
	}  
}
