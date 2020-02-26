package wide_column

import DataModel._

final case object TableUtil {
	def get_object_desc(object_name: String,
	                    table: TABLE):
	Option[OBJECT_TYPE_DESC] = table.table_desc.object_type.get(object_name)
	
	def add_data_map(key0: String,
	                 data_map0: DATA_MAP,
								   data_map1: DATA_MAP):
	DATA_MAP = DataModelUtil.is_empty(data_map0) match {
		case false => data_map1 ++ List((key0, data_map0), (DataModel.node_type, DataModel.NODE))
		case true  => {val dmap1 = data_map1 - key0
			DataModelUtil.is_empty(dmap1) match {
				case false => dmap1
				case true  => DataModelUtil.empty_map
			}
		}
	}
	
	def get_map_list(primary_key: PRIMARY_KEY,
	                 map: DATA_MAP):
	List[MapDataStatus] = {
		val (l1, _) = primary_key.primary_key.foldLeft((List.empty[MapDataStatus], map)){
			case ((l0, m0), av0) => {
				val qv0 = Attribute.get_qualified_value(av0)
				val m1 = DataModelUtil.get(qv0, m0)
				(m1 :: l0, m1.data_map)
			}
		}
		l1
	}
	
	def get_leaf(primary_key: PRIMARY_KEY,
	            table: TABLE):
	Option[DATA_MAP] =
	  get_map_list(primary_key, table.table_data).headOption.fold(None: Option[DATA_MAP])(m1 => Some(m1.data_map))
		
  def make_attribute(attribute_list: List[ATTRIBUTE_VALUE],
		                 uaf: (ATTRIBUTE_VALUE, DATA_MAP) => DATA_MAP,
		                 map: DATA_MAP):
  DATA_MAP = attribute_list.foldLeft(map)((map0, av0) => uaf(av0, map0))
	
  def update_data_map(primary_key: PRIMARY_KEY, 
		                  data_map: DATA_MAP,
						          table: TABLE):
  TABLE = {
  	val (_, key2, map2) = get_map_list(primary_key, table.table_data).foldLeft((true, DataModel.str_attr_list, DataModelUtil.empty_map)){
  		case ((true, _, _),        m1) => (false, m1.data_key, data_map.updated(DataModel.node_type, DataModel.LEAF))
			case ((false, key0, map0), m1) => (false, m1.data_key, add_data_map(key0, map0, m1.data_map))
  	}
		table.copy(table_data = add_data_map(key2, map2, table.table_data))
  } 
}