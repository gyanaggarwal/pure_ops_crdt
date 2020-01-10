package pure_ops
package implementation

final case object EWFlag extends OWFlag {
	val init_data: Boolean = true
	
	def combine_any(value0: Any,
								  value1: Any):
	Any = (value0, value1) match {
		case ((true, true), _)  => (true, true)
		case (_, (true, true))  => (true, true)
		case ((true, false), _) => (true, false)
		case (_, (true, false)) => (true, false)
		case _                  => value0
	} 
}