package wide_column

import DataModel._

final case object Table {
	val str_attr_list = "attribute_list"
	
	def create_table_desc(table_name: String, 
		                    object_type_list: List[OBJECT_TYPE_DESC]):
	TABLE_DESC = {
		val object_type = object_type_list.foldLeft(DataModelUtil.empty_map){
			case (map0, object_type_desc0) => map0.updated(ObjectType.get_object_name(object_type_desc0), object_type_desc0)
		}
		TABLE_DESC(table_name, object_type.asInstanceOf[Map[String, OBJECT_TYPE_DESC]])
	}
	
	def create_table(table_desc: TABLE_DESC):
  TABLE = TABLE(table_desc, DataModelUtil.empty_map)
		
  def update_attribute(object_name: String,
		                   primary_key: PRIMARY_KEY, 
		                   attribute_list: List[ATTRIBUTE_VALUE], 
						           table: TABLE):
  TABLE = set_attribute(object_name, primary_key, attribute_list, Attribute.update_attribute, table)

  def delete_attribute(object_name: String,
		                   primary_key: PRIMARY_KEY,
	                     attribute_list: List[ATTRIBUTE_VALUE],
										   table: TABLE):
	TABLE = set_attribute(object_name, primary_key, attribute_list, Attribute.delete_attribute, table)

	def delete(object_name: String,
		         primary_key: PRIMARY_KEY, 
						 table: TABLE):
  TABLE = {
		val table1 = delete_secondary_index(object_name, primary_key, table)
		val map_list = get_map_list(primary_key, table1.table_data)
  	val (data_status2, _, key2, map2) = 
		  map_list.foldLeft((EXISTS_STATUS: DATA_STATUS, true, str_attr_list, DataModelUtil.empty_map)){
  		  case (_, MapDataStatus(EMPTY_STATUS, _, _))   => (EMPTY_STATUS, true, str_attr_list, DataModelUtil.empty_map)
				case ((EMPTY_STATUS, _, _, _), _)             => (EMPTY_STATUS, true, str_attr_list, DataModelUtil.empty_map)
				case ((EXISTS_STATUS, true, _, _), m1)        => (EXISTS_STATUS, false, m1.data_key, DataModelUtil.empty_map)
				case ((EXISTS_STATUS, false, key0, map0), m1) => (EXISTS_STATUS, false, m1.data_key, add_data_map(key0, map0, m1.data_map))
  	  }
			
		data_status2 match {
			case EXISTS_STATUS => global_compute_table(object_name, 
				                                         primary_key, 
																								 table1.copy(table_data = add_data_map(key2, map2, table1.table_data)))
			case _             => table
		}
  }

  def get(primary_key: PRIMARY_KEY, 
		      table: TABLE):
  Option[DATA_MAP] = get_map_list(primary_key, table.table_data).headOption.fold(None: Option[DATA_MAP]){
  	case MapDataStatus(EMPTY_STATUS, _, _)         => None: Option[DATA_MAP]
		case MapDataStatus(EXISTS_STATUS, _, data_map) => DataModelUtil.is_empty(data_map) match {
			case true  => None: Option[DATA_MAP]
			case false => Some(data_map)
		}
  }
	
/* private functions*/	
	def add_data_map(key0: String,
	                 data_map0: DATA_MAP,
								   data_map1: DATA_MAP):
	DATA_MAP = DataModelUtil.is_empty(data_map0) match {
		case false => data_map1 ++ List((key0, data_map0), (DataModel.node_type, DataModel.NODE))
		case true  => data_map1 - key0
	}
	
  def make_attribute(attribute_list: List[ATTRIBUTE_VALUE],
		                 uaf: (ATTRIBUTE_VALUE, DATA_MAP) => DATA_MAP,
		                 map: DATA_MAP):
  DATA_MAP = attribute_list.foldLeft(map)((map0, attr_value0) => uaf(attr_value0, map0))
	
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

  def update_data_map(primary_key: PRIMARY_KEY, 
		                  data_map: DATA_MAP,
						          table: TABLE):
  TABLE = {
  	val (_, key2, map2) = get_map_list(primary_key, table.table_data).foldLeft((true, str_attr_list, DataModelUtil.empty_map)){
  		case ((true, _, _),        m1) => (false, m1.data_key, data_map.updated(DataModel.node_type, DataModel.LEAF))
			case ((false, key0, map0), m1) => (false, m1.data_key, add_data_map(key0, map0, m1.data_map))
  	}
		table.copy(table_data = add_data_map(key2, map2, table.table_data))
  } 
		
  def set_attribute(object_name: String,
		                primary_key: PRIMARY_KEY, 
		                attribute_list: List[ATTRIBUTE_VALUE], 
										uaf: (ATTRIBUTE_VALUE, DATA_MAP) => DATA_MAP,
						        table: TABLE):
  TABLE = update_dependency(object_name, 
		                        primary_key, 
														get_map_list(primary_key, table.table_data).headOption.fold(table)(
															m1 => update_data_map(primary_key, 
							                                      make_attribute(attribute_list, uaf, m1.data_map), 
																							      table)))
	
	def local_compute(attribute_desc: LCOMPUTE_ATTRIBUTE_DESC,
	                  data_map: DATA_MAP):
	DATA_MAP = {
		val qname = Attribute.get_qualified_name(attribute_desc)
		val lcompute_input = Attribute.get_lcompute_input(attribute_desc)
		lcompute_input.compute_function.compute(lcompute_input.attribute_list,
	                                          List(data_map),
																						lcompute_input.value_function).
		  fold(data_map - qname)(value => data_map.updated(qname, value))																										 
	}
										
	def local_compute_list(lcompute_list: List[LCOMPUTE_ATTRIBUTE_DESC],
	                       data_map: DATA_MAP):
	DATA_MAP = lcompute_list.foldLeft(data_map)((map0, attr_desc0) => local_compute(attr_desc0, map0))
	
	def local_compute_attribute(primary_key: PRIMARY_KEY,
	                            lcompute_list: List[LCOMPUTE_ATTRIBUTE_DESC],
														  table: TABLE):
	TABLE = get(primary_key, table).fold(table)(data_map => update_data_map(primary_key,
												                                                  local_compute_list(lcompute_list, data_map),
																									                        table))
	
	def global_compute(attribute_desc: GAGGREGATE_ATTRIBUTE_DESC,
	                   primary_key: PRIMARY_KEY,
										 table: TABLE):
	TABLE = Attribute.get_gaggregate_input(attribute_desc).global_aggregate_function.compute(attribute_desc, 
	                                                                                         primary_key,
																																												   table) 
	def global_compute_list(primary_key: PRIMARY_KEY, 
		                      attribute_list: List[GAGGREGATE_ATTRIBUTE_DESC],
													table: TABLE):
	TABLE = attribute_list.foldLeft(table)((t0, av0) => global_compute(av0, primary_key, t0))
																																													 														
	def global_compute_table(object_name: String,
	                         primary_key: PRIMARY_KEY,
												   table: TABLE):
	TABLE = table.table_desc.object_type.get(object_name).fold(table){
	  case ENTITY_TX_DESC(_, _, _, gcompute_list, _) => global_compute_list(primary_key, gcompute_list, table)
		case _                                         => table
	}

	def update_secondary_index(data_map: DATA_MAP,
	                           index_desc: INDEX_DESC,
														 update_flag: Boolean,
													   table: TABLE):
	TABLE = {
	  (update_flag, PrimaryKey.make_primary_key(index_desc.primary_key, data_map)) match {
	  	case (true, (Some(upk), _))  => update_attribute(index_desc.object_name, 
				                                               upk, 
																							         DataModelUtil.make_attribute_value_list(index_desc.attribute_list, data_map),
																						           table)
			case (true, (_, Some(dpk)))  => delete(index_desc.object_name, 
				                                     index_desc.delete_primary_key.make(dpk),
																	           table)
			case (false, (Some(dpk), _)) => delete(index_desc.object_name, 
				                                     dpk,
																	           table)
			case _                       => table
	  }
	}
		
	def update_secondary_index(data_map: DATA_MAP,
	                           index_list: List[INDEX_DESC],
														 update_flag: Boolean,
													   table: TABLE):
	TABLE = index_list.foldLeft(table)((t0, id0) => update_secondary_index(data_map, id0, update_flag, t0))	

  def update_secondary_index(primary_key: PRIMARY_KEY,
	                           index_list: List[INDEX_DESC],
													   update_flag: Boolean,
													   table: TABLE):
	TABLE = get(primary_key, table).fold(table)(data_map => update_secondary_index(data_map, index_list, update_flag, table))
	
  def update_dependency(object_name: String,
	                      primary_key: PRIMARY_KEY,
											  table: TABLE):
	TABLE = table.table_desc.object_type.get(object_name).fold(table){
		case ENTITY_TX_DESC(_, _, lcompute_list, gcompute_list, index_list) => 
		  global_compute_list(primary_key, gcompute_list,
												  update_secondary_index(primary_key, index_list, true, 
														                     local_compute_attribute(primary_key, lcompute_list, table)))
		case _                                                              => table       
	}
																																													 
	def delete_secondary_index(object_name: String,
														 primary_key: PRIMARY_KEY,
														 table: TABLE):
	TABLE = table.table_desc.object_type.get(object_name).fold(table){
		case ENTITY_TX_DESC(_, _, _, _, index_list) => update_secondary_index(primary_key, index_list, false, table)
		case _                                      => table
	}
}