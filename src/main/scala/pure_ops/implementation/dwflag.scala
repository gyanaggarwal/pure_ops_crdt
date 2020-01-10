package pure_ops
package implementation

final case object DWFlag extends OWFlag {	
  val init_data: Boolean = true
	
  def combine_any(value0: Any,
		  value1: Any):
  Any = value0.asInstanceOf[Boolean] && value1.asInstanceOf[Boolean]
}
