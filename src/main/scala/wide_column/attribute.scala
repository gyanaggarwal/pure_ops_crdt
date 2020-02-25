package wide_column

import DataModel._

final case object Attribute {
	def make_attribute_desc(object_name:    String, 
		                      attribute_name: String):
	ATTRIBUTE_DESC = QUALIFIED_ATTRIBUTE_DESC(object_name, 
	                                          attribute_name,
																					  DataModelUtil.qualified_value(object_name, attribute_name))
																						
	def make_attribute_desc(object_name:    String, 
		                      attribute_name: String,
													compute_input:  LOCAL_INPUT):
	LOCAL_ATTRIBUTE_DESC = LOCAL_ATTRIBUTE_DESC(make_attribute_desc(object_name, attribute_name), compute_input)
	
	def make_attribute_desc(object_name:    String, 
		                      attribute_name: String,
													compute_input:  GLOBAL_INPUT):
	GLOBAL_ATTRIBUTE_DESC = GLOBAL_ATTRIBUTE_DESC(make_attribute_desc(object_name, attribute_name), compute_input)
	
	def make_key_desc(object_name: String, 
		                attribute_name: String):
	KEY_DESC = QUALIFIED_KEY_DESC(object_name, 
																attribute_name,
																DataModelUtil.qualified_value(object_name, attribute_name))
																
	def get_qualified_name(attribute_desc: ATTRIBUTE_DESC):
	String = attribute_desc match {
		case QUALIFIED_ATTRIBUTE_DESC(_, _, qualified_name) => qualified_name
	}
	
	def get_qualified_name(attribute_desc: DERIVED_ATTRIBUTE_DESC):
	String = attribute_desc match {
		case LOCAL_ATTRIBUTE_DESC(ad, _)  => get_qualified_name(ad)
		case GLOBAL_ATTRIBUTE_DESC(ad, _) => get_qualified_name(ad)
	}
	
	def get_qualified_name(key_desc: KEY_DESC):
	String = key_desc match {
		case QUALIFIED_KEY_DESC(_, _, qualified_name) => qualified_name
		case VALUE_KEY_DESC(value)                    => value.toString
	}
	
	def get_qualified_name(attribute_value: ATTRIBUTE_VALUE):
	String = attribute_value match {
		case ANY_ATTRIBUTE_VALUE(attribute, _) => get_qualified_name(attribute)
		case MAP_ATTRIBUTE_VALUE(attribute, _) => get_qualified_name(attribute)
	}
	
	def get_qualified_name(key_value: KEY_VALUE):
	String = key_value match {
		case ATTRIBUTE_KEY_VALUE(attribute, _) => get_qualified_name(attribute)
		case VALUE_KEY_VALUE(value)            => value.toString
		case MISSING_KEY_VALUE()               => ""
	}
	
	def get_value(attribute_value: ATTRIBUTE_VALUE):
	Any = attribute_value match {
		case ANY_ATTRIBUTE_VALUE(_, value) => value
		case MAP_ATTRIBUTE_VALUE(_, value) => value
	}
	
	def get_qualified_value(key_value: KEY_VALUE):
	String = key_value match {
		case ATTRIBUTE_KEY_VALUE(attribute, value) => DataModelUtil.qualified_value(get_qualified_name(attribute), value)
		case VALUE_KEY_VALUE(value)                => value.toString
		case MISSING_KEY_VALUE()                   => ""
	}

	def update_attribute(attribute_value: ATTRIBUTE_VALUE, map: DATA_MAP):
	DATA_MAP = {
		val qn = get_qualified_name(attribute_value)
		val v0 = get_value(attribute_value)
		val v3 = attribute_value match {
			case _: ANY_ATTRIBUTE_VALUE => v0
		  case _: MAP_ATTRIBUTE_VALUE => {
				val v1 = v0.asInstanceOf[MAP_ATTR_DATA]
				map.get(qn).fold(v1)(v2 => v2.asInstanceOf[MAP_ATTR_DATA] ++ v1)
	    }
		}
		map.updated(qn, v3)
  }
	
	def delete_attribute(attribute_value: ATTRIBUTE_VALUE, map: DATA_MAP):
	DATA_MAP = {
		val qn = get_qualified_name(attribute_value)
		val v0 = get_value(attribute_value)
		attribute_value match {
			case _: ANY_ATTRIBUTE_VALUE => map - qn
		  case _: MAP_ATTRIBUTE_VALUE => map.get(qn).fold(map)(v1 => 
																			 map.updated(qn, v1.asInstanceOf[MAP_ATTR_DATA] -- v0.asInstanceOf[MAP_ATTR_DATA].keys))
	  }
  }	
}