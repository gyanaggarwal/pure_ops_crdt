package pure_ops
package implementation

final case object WCDataModel extends PureOpsNonCommCRDT {
  def combine_msg_log_data(msg_log_data: Any,crdt_data: Any): Any = ???
  def init_data: Any = ???
  def valid_ops(ops: pure_ops.CRDTOps): Boolean = ???
	def combine_any(value0: Any,value1: Any): Any = ???
  def init_any: Any = ???
  def isConcurrent(crdt_ops0: pure_ops.CRDTOps,crdt_ops1: pure_ops.CRDTOps): Boolean = ???
  def update_any(value: Any,crdt_ops: pure_ops.CRDTOps): Any = ???
}
