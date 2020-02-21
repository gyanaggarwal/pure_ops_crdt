package wide_column

import DataModel._

final case object Attribute {
	
	def make_qualified_desc(object_name: String,
	                        attribute_name: String):
	ATTRIBUTE_KEY_DESC = QUALIFIED_ATTRIBUTE_DESC(object_name, 
		                                            attribute_name, 
																						    DataModelUtil.qualified_value(object_name, attribute_name))
																						
	def make_qualified_desc(object_name: String,
													attribute_name: String,
													lcompute_input: LCOMPUTE_INPUT):
	LCOMPUTE_ATTRIBUTE_DESC = LCOMPUTE_QUALIFIED_ATTRIBUTE_DESC(make_qualified_desc(object_name, 
		                                                                              attribute_name).asInstanceOf[QUALIFIED_ATTRIBUTE_DESC],
																															lcompute_input)
																						
	def make_qualified_desc(object_name: String,
													attribute_name: String,
													gaggregate_input: GAGGREGATE_INPUT):
	GAGGREGATE_ATTRIBUTE_DESC = GAGGREGATE_QUALIFIED_ATTRIBUTE_DESC(make_qualified_desc(object_name, 
		                                                                                  attribute_name).asInstanceOf[QUALIFIED_ATTRIBUTE_DESC],
																																	gaggregate_input)
	def get_qualified_name(attribute_desc: ATTRIBUTE_DESC):
	String = attribute_desc match {
		case _: VALUE_ATTRIBUTE                     => attribute_desc.asInstanceOf[VALUE_ATTRIBUTE].value.toString
		case _: QUALIFIED_ATTRIBUTE_DESC            => attribute_desc.asInstanceOf[QUALIFIED_ATTRIBUTE_DESC].qualified_name
		case _: LCOMPUTE_QUALIFIED_ATTRIBUTE_DESC   => get_qualified_name(attribute_desc.asInstanceOf[LCOMPUTE_QUALIFIED_ATTRIBUTE_DESC].qualified_desc)
		case _: GAGGREGATE_QUALIFIED_ATTRIBUTE_DESC => get_qualified_name(attribute_desc.asInstanceOf[GAGGREGATE_QUALIFIED_ATTRIBUTE_DESC].qualified_desc)
	}
	
	def get_lcompute_input(attribute_desc: LCOMPUTE_ATTRIBUTE_DESC):
	LCOMPUTE_INPUT = attribute_desc match {
		case _: LCOMPUTE_QUALIFIED_ATTRIBUTE_DESC => attribute_desc.asInstanceOf[LCOMPUTE_QUALIFIED_ATTRIBUTE_DESC].lcompute_input
	}
	
	def get_gaggregate_input(attribute_desc: GAGGREGATE_ATTRIBUTE_DESC):
	GAGGREGATE_INPUT = attribute_desc match {
		case _: GAGGREGATE_QUALIFIED_ATTRIBUTE_DESC => attribute_desc.asInstanceOf[GAGGREGATE_QUALIFIED_ATTRIBUTE_DESC].gaggregate_input
	}

	def get_qualified_desc(attribute_desc: ATTRIBUTE_DESC):
	ATTRIBUTE_DESC = attribute_desc match {
		case _: LCOMPUTE_QUALIFIED_ATTRIBUTE_DESC   => attribute_desc.asInstanceOf[LCOMPUTE_QUALIFIED_ATTRIBUTE_DESC].qualified_desc
		case _: GAGGREGATE_QUALIFIED_ATTRIBUTE_DESC => attribute_desc.asInstanceOf[GAGGREGATE_QUALIFIED_ATTRIBUTE_DESC].qualified_desc
		case _                                      => attribute_desc		
	}
	
	def get_value(attribute_value: ATTRIBUTE_VALUE):
	Any = attribute_value match {
		case _: VALUE_ATTRIBUTE         => attribute_value.asInstanceOf[VALUE_ATTRIBUTE].value
		case _: MISSING_ATTRIBUTE_VALUE => attribute_value.asInstanceOf[MISSING_ATTRIBUTE_VALUE].value
		case _: MAP_ATTRIBUTE_VALUE     => attribute_value.asInstanceOf[MAP_ATTRIBUTE_VALUE].value
		case _: ANY_ATTRIBUTE_VALUE     => attribute_value.asInstanceOf[ANY_ATTRIBUTE_VALUE].value
		case _: KEY_ATTRIBUTE_VALUE     => attribute_value.asInstanceOf[KEY_ATTRIBUTE_VALUE].value
	}

	def get_qualified_name(attribute_value: ATTRIBUTE_VALUE):
	String = attribute_value match {
		case _: VALUE_ATTRIBUTE         => attribute_value.asInstanceOf[VALUE_ATTRIBUTE].value.toString
		case _: MISSING_ATTRIBUTE_VALUE => attribute_value.asInstanceOf[MISSING_ATTRIBUTE_VALUE].value.toString
		case _: MAP_ATTRIBUTE_VALUE     => get_qualified_name(attribute_value.asInstanceOf[MAP_ATTRIBUTE_VALUE].attribute)
		case _: ANY_ATTRIBUTE_VALUE     => get_qualified_name(attribute_value.asInstanceOf[ANY_ATTRIBUTE_VALUE].attribute)
		case _: KEY_ATTRIBUTE_VALUE     => get_qualified_name(attribute_value.asInstanceOf[KEY_ATTRIBUTE_VALUE].attribute)
	}
	
	def get_qualified_value(attribute_value: ATTRIBUTE_KEY_VALUE):
	String = attribute_value match {
		case _: VALUE_ATTRIBUTE         => attribute_value.asInstanceOf[VALUE_ATTRIBUTE].value.toString		
		case _: MISSING_ATTRIBUTE_VALUE => attribute_value.asInstanceOf[MISSING_ATTRIBUTE_VALUE].value.toString
		case _                          => DataModelUtil.qualified_value(get_qualified_name(attribute_value), get_value(attribute_value))
	}	
	
	def update_attribute(attribute_value: ATTRIBUTE_VALUE, map: DATA_MAP):
	DATA_MAP = {
		val qn = get_qualified_name(attribute_value)
		val v0 = get_value(attribute_value)
		val v2 = attribute_value match {
		  case _: MAP_ATTRIBUTE_VALUE => map.get(qn).fold(v0.asInstanceOf[MAP_ATTR_DATA])(v1 => 
																			 v1.asInstanceOf[MAP_ATTR_DATA] ++ v0.asInstanceOf[MAP_ATTR_DATA])
		  case _                      => v0
	  }
		map.updated(qn, v2)
  }
	
	def delete_attribute(attribute_value: ATTRIBUTE_VALUE, map: DATA_MAP):
	DATA_MAP = {
		val qn = get_qualified_name(attribute_value)
		val v0 = get_value(attribute_value)
		attribute_value match {
		  case _: MAP_ATTRIBUTE_VALUE => map.get(qn).fold(map)(v1 => 
																			 map.updated(qn, v1.asInstanceOf[MAP_ATTR_DATA] -- v0.asInstanceOf[MAP_ATTR_DATA].keys))
		  case _                      => map - qn
	  }
  }
}