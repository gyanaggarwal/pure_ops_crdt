package wide_column

import DataModel._

final case object PrimaryKey {
	def make_primary_key(primary_key_desc: PRIMARY_KEY_DESC,
	                     primary_key: PRIMARY_KEY):
	Option[PRIMARY_KEY] = {
		val olist = primary_key_desc.primary_key.foldLeft(Some(List.empty[ATTRIBUTE_KEY_VALUE]): Option[List[ATTRIBUTE_KEY_VALUE]]){
			case (Some(l0), ad0: VALUE_ATTRIBUTE) => Some(ad0 :: l0)
			case (Some(l0), ad0)                  => DataModelUtil.find(ad0, primary_key.primary_key) match {
				case Some(akv) => Some(akv :: l0)
				case None      => None: Option[List[ATTRIBUTE_KEY_VALUE]]
			}
			case (None, _)                        => None: Option[List[ATTRIBUTE_KEY_VALUE]]
		}
		
		olist match {
			case Some(_) => Some(PRIMARY_KEY(olist.get.reverse))
			case None    => None: Option[PRIMARY_KEY]
		}
	}
	
	def make_primary_key(primary_key_desc: PRIMARY_KEY_DESC,
	                     data_map: DATA_MAP):
	(Option[PRIMARY_KEY], Option[PRIMARY_KEY]) = {
		val (flag, list) = primary_key_desc.primary_key.foldLeft((true, List.empty[ATTRIBUTE_KEY_VALUE])){
			case ((flag0, l0), ad0: VALUE_ATTRIBUTE) => (flag0, ad0 :: l0)
			case ((flag0, l0), ad0)                  => data_map.get(Attribute.get_qualified_name(ad0)).
			                                              fold((false, DataModel.missing_key_value :: l0))(value =>
																										(flag0, KEY_ATTRIBUTE_VALUE(Attribute.get_qualified_desc(ad0), value) :: l0))
		}
		val primary_key = PRIMARY_KEY(list.reverse)
		flag match {
			case true  => (Some(primary_key), None: Option[PRIMARY_KEY])
			case false => (None: Option[PRIMARY_KEY], Some(primary_key))
		}
	}
}