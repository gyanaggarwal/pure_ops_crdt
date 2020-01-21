package pure_ops
package implementation

final case object MVRegister extends PureOpsNonCommCRDT {
	val init_data: List[Any] = List.empty[Any]

	val init_any = (WRT(), init_data)
	
	def valid_ops(crdt_ops: CRDTOps): 
	Boolean = crdt_ops match {
		case WRT(_) => true
		case CLR(_) => true
		case _      => false
	}
	
  def isConcurrent(crdt_ops0: CRDTOps, crdt_ops1: CRDTOps):
	Boolean = true
	
	def update_any(value: Any,
	               crdt_ops: CRDTOps):
	Any = (value, crdt_ops) match {
		case ((_, l0), WRT(args)) => (WRT(), List(args))
		case (_, CLR(_))          => (CLR(), init_data)
		case _                    => value
	}
								
	def combine_any(value0: Any,
									value1: Any):
	Any = (value0, value1) match {
		case ((CLR(_), _), (CLR(_), _)) => (CLR(), init_data)
		case ((_, l0), (_, l1))         => (WRT(), l0.asInstanceOf[List[Any]] ++ l1.asInstanceOf[List[Any]])
		case _                          => init_any
	} 
	
  def combine_msg_log_data(msg_log_data: Any,
	                         crdt_data: Any): 
	Any = (msg_log_data, crdt_data) match {
		case ((CLR(_), _), _)   => init_data
		case ((WRT(_), l0), _)  => l0
		case _                  => crdt_data
	}
	
}