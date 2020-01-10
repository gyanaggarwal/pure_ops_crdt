package pure_ops
package implementation

trait OWFlag extends PureOpsNonCommCRDT {
  def init_data: Boolean
	
  val init_any = init_data
	
  def valid_ops(crdt_ops: CRDTOps): 
  Boolean = crdt_ops match {
    case SET(_) => true
    case _      => false
  }
	
  def update_any(value: Any,
	         crdt_ops: CRDTOps):
  Any = crdt_ops match {
    case SET(args) => args
    case _         => value
  }
	
  def combine_msg_log_data(msg_log_data: Any,
	                   crdt_data: Any): 
  Any = combine_any(msg_log_data, crdt_data)
}
