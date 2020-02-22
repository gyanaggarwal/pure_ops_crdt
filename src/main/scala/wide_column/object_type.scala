package wide_column

import DataModel._

final case object ObjectType {	
	def get_object_name(object_type_desc: OBJECT_TYPE_DESC):
	String = object_type_desc match {
		case _: ENTITY_TX_DESC => object_type_desc.asInstanceOf[ENTITY_TX_DESC].object_name
		case _: INDEX_DESC     => object_type_desc.asInstanceOf[INDEX_DESC].object_name
	}
}