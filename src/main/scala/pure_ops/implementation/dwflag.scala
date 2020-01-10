package pure_ops
package implementation

final case object DWFlag extends OWFlag {	
	val init_data: Boolean = false
	
	def combine_any(value0: Any,
								  value1: Any):
	Any = (value0, value1) match {
		case ((true, false), _) => (true, false)
		case (_, (true, false)) => (true, false)
		case ((true, true), _)  => (true, true)
		case (_, (true, true))  => (true, true)
	  case _                  => value0
	}
}