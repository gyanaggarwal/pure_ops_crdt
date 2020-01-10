package pure_ops
package implementation

trait OWFlag extends PureOpsNonCommCRDT {
	def init_data: Boolean
	
	val init_any = (false, init_data)
	
	def valid_ops(crdt_ops: CRDTOps): 
	Boolean = crdt_ops match {
		case SET(_) => true
		case _      => false
	}
	
	def update_any(value: Any,
	               crdt_ops: CRDTOps):
	Any = crdt_ops match {
		case SET(args) => (true, args)
		case _         => value
	}
	
  def combine_msg_log_data(msg_log_data: Any,
	                         crdt_data: Any): 
	Any = msg_log_data match {
		case (true, data) => data
		case _            => crdt_data
	} 
}