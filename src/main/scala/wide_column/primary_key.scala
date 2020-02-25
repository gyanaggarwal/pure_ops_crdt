package wide_column

import DataModel._

final case object PrimaryKey {
	def make_primary_key(primary_key_desc: PRIMARY_KEY_DESC,
	                     primary_key: PRIMARY_KEY):
	Option[PRIMARY_KEY] = {
		val olist = primary_key_desc.primary_key.foldLeft(Some(List.empty[KEY_VALUE]): Option[List[KEY_VALUE]]){
			case (Some(l0), ad0: QUALIFIED_KEY_DESC) => DataModelUtil.find(ad0, primary_key.primary_key) match {
				case Some(kv) => Some(kv :: l0)
				case None     => None: Option[List[KEY_VALUE]]
			}
			case (Some(l0), VALUE_KEY_DESC(value))   => Some(VALUE_KEY_VALUE(value) :: l0)
			case (None, _)                           => None: Option[List[KEY_VALUE]]
		}
		
		olist match {
			case Some(_) => Some(PRIMARY_KEY(olist.get.reverse))
			case None    => None: Option[PRIMARY_KEY]
		}
	}
	
	def make_primary_key(primary_key_desc: PRIMARY_KEY_DESC,
	                     data_map: DATA_MAP):
	(Option[PRIMARY_KEY], Option[PRIMARY_KEY]) = {
		val (flag, list) = primary_key_desc.primary_key.foldLeft((true, List.empty[KEY_VALUE])){
			case ((flag0, l0), ad0: QUALIFIED_KEY_DESC) => data_map.get(Attribute.get_qualified_name(ad0)).
			                                                 fold((false, DataModel.missing_key_value :: l0))(value =>
																										     (flag0, ATTRIBUTE_KEY_VALUE(ad0, value) :: l0))
			case ((flag0, l0), VALUE_KEY_DESC(value))   => (flag0, VALUE_KEY_VALUE(value) :: l0)
		}
		val primary_key = PRIMARY_KEY(list.reverse)
		flag match {
			case true  => (Some(primary_key), None: Option[PRIMARY_KEY])
			case false => (None: Option[PRIMARY_KEY], Some(primary_key))
		}
	}
}