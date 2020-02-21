package wide_column

import DataModel._

final case object ObjectType {
	
	def get_object_name(object_type_desc: OBJECT_TYPE_DESC):
	String = object_type_desc match {
		case _: ENTITY_TX_DESC => object_type_desc.asInstanceOf[ENTITY_TX_DESC].object_name
		case _: INDEX_DESC     => object_type_desc.asInstanceOf[INDEX_DESC].object_name
	}
	
	def get_lcompute_list(object_type_desc: OBJECT_TYPE_DESC):
	Option[List[LCOMPUTE_ATTRIBUTE_DESC]] = object_type_desc match {
		case _: ENTITY_TX_DESC => Some(object_type_desc.asInstanceOf[ENTITY_TX_DESC].lcompute_list)
		case _: INDEX_DESC     => None: Option[List[LCOMPUTE_ATTRIBUTE_DESC]]
	}
	
	def get_gcompute_list(object_type_desc: OBJECT_TYPE_DESC):
	Option[List[GAGGREGATE_ATTRIBUTE_DESC]] = object_type_desc match {
		case _: ENTITY_TX_DESC => Some(object_type_desc.asInstanceOf[ENTITY_TX_DESC].gcompute_list)
		case _: INDEX_DESC     => None: Option[List[GAGGREGATE_ATTRIBUTE_DESC]]
	}
	
	def get_index_list(object_type_desc: OBJECT_TYPE_DESC):
	Option[List[INDEX_DESC]] = object_type_desc match {
		case _: ENTITY_TX_DESC => Some(object_type_desc.asInstanceOf[ENTITY_TX_DESC].index_list)
		case _: INDEX_DESC     => None: Option[List[INDEX_DESC]]
	}
}