package pure_ops
package implementation

final case object EWFlag extends OWFlag {
  val init_data: Boolean = false
	
  def combine_any(value0: Any,
		  value1: Any):
  Any = value0.asInstanceOf[Boolean] || value1.asInstanceOf[Boolean]
}
