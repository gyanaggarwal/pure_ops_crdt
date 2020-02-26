package wide_column

import DataModel._

final case object Table {
	def create_table_desc(table_name: String, 
		                    object_type_list: List[OBJECT_TYPE_DESC]):
	TABLE_DESC = {
		val object_type = object_type_list.foldLeft(DataModelUtil.empty_object_type_map){
			case (map0, object_type_desc0) => map0.updated(ObjectType.get_object_name(object_type_desc0), object_type_desc0)
		}
		TABLE_DESC(table_name, object_type)
	}
	
	def create_table(table_desc: TABLE_DESC):
  TABLE = TABLE(table_desc, DataModelUtil.empty_map)

  def get(primary_key: PRIMARY_KEY, 
		      table: TABLE):
  Option[DATA_MAP] = TableUtil.get_map_list(primary_key, table.table_data).headOption.fold(None: Option[DATA_MAP]){
  	case MapDataStatus(EMPTY_STATUS, _, _)         => None: Option[DATA_MAP]
		case MapDataStatus(EXISTS_STATUS, _, data_map) => DataModelUtil.is_empty(data_map) match {
			case true  => None: Option[DATA_MAP]
			case false => Some(data_map)
		}
  }

  def get_list(primary_key: PRIMARY_KEY,
	             table: TABLE):
	List[DATA_MAP] = get(primary_key, table).fold(List.empty[DATA_MAP])(data_map => 
		DataModelUtil.collect_leaf(data_map).map(dmp => dmp - DataModel.node_type))
	
	def delete(object_name: String,
		         primary_key: PRIMARY_KEY, 
						 table: TABLE):
  TABLE = {
		val table1 = delete_dependency(object_name, primary_key, table)
		val map_list = TableUtil.get_map_list(primary_key, table1.table_data)
  	val (data_status2, _, key2, map2) = 
		  map_list.foldLeft((EXISTS_STATUS: DATA_STATUS, true, DataModel.str_attr_list, DataModelUtil.empty_map)){
  		  case (_, MapDataStatus(EMPTY_STATUS, _, _))   => (EMPTY_STATUS, true, str_attr_list, DataModelUtil.empty_map)
				case ((EMPTY_STATUS, _, _, _), _)             => (EMPTY_STATUS, true, str_attr_list, DataModelUtil.empty_map)
				case ((EXISTS_STATUS, true, _, _), m1)        => (EXISTS_STATUS, false, m1.data_key, DataModelUtil.empty_map)
				case ((EXISTS_STATUS, false, key0, map0), m1) => (EXISTS_STATUS, false, m1.data_key, TableUtil.add_data_map(key0, map0, m1.data_map))
  	  }
			
		data_status2 match {
			case EXISTS_STATUS => global_compute(object_name, 
				                                   primary_key, 
																					 table1.copy(table_data = TableUtil.add_data_map(key2, map2, table1.table_data)))
			case _             => table
		}
  }

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

  private def set_attribute(object_name: String,
		                        primary_key: PRIMARY_KEY, 
		                        attribute_list: List[ATTRIBUTE_VALUE], 
										        uaf: (ATTRIBUTE_VALUE, DATA_MAP) => DATA_MAP,
						                table: TABLE):
	TABLE = update_dependency(object_name,
		                        primary_key,
														TableUtil.get_leaf(primary_key, table).
	                            fold(table)(data_map => 
																TableUtil.update_data_map(primary_key, 
							                                            TableUtil.make_attribute(attribute_list, uaf, data_map), 
																							            table)))
																													
	private def local_compute(attribute_desc: LOCAL_ATTRIBUTE_DESC,
	                          data_map: DATA_MAP):
	DATA_MAP = attribute_desc.compute_input.compute_function.compute(attribute_desc.compute_input.attribute_list,
	                                                                 attribute_desc.attribute_desc,
																																   attribute_desc.compute_input.check_function,
																																   attribute_desc.compute_input.value_function,
																																   data_map)
																																	 
	private def local_compute(local_list: List[LOCAL_ATTRIBUTE_DESC],
	                          data_map: DATA_MAP):
	DATA_MAP = local_list.foldLeft(data_map)((dm0, ad0) => local_compute(ad0, dm0))	
						
	private def local_compute(primary_key: PRIMARY_KEY,
	                          local_list: List[LOCAL_ATTRIBUTE_DESC],
													  table: TABLE):
	TABLE = TableUtil.get_leaf(primary_key, table).fold(table)(data_map => 
						TableUtil.update_data_map(primary_key, 
							                        local_compute(local_list, data_map), 
																			table))
	
	private def delete_dependency(object_name: String,
	                              primary_key: PRIMARY_KEY,
												        table: TABLE):
	TABLE = TableUtil.get_object_desc(object_name, table).fold(table){
		case ENTITY_TX_DESC(_, _, _, _, index_list, detail_list) => 
		  update_index(primary_key, index_list, false, delete_detail(primary_key, detail_list, table))
		case _: INDEX_DESC                             => table
	}
	
	private def global_compute(attribute_desc: GLOBAL_ATTRIBUTE_DESC,
		                         primary_key: PRIMARY_KEY,
													   table: TABLE):
	TABLE = attribute_desc.compute_input.compute_function.compute(attribute_desc.compute_input.src_key_desc,
	                                                              attribute_desc.compute_input.trg_key_desc,
																															  attribute_desc.compute_input.trg_object_name,
																															  attribute_desc.attribute_desc,
																															  attribute_desc.compute_input.value_function,
																															  primary_key,
																															  table)
	
	private def global_compute(primary_key: PRIMARY_KEY,
														 attribute_list: List[GLOBAL_ATTRIBUTE_DESC],
													   table: TABLE):
	TABLE = attribute_list.foldLeft(table)((t0, ad0) => global_compute(ad0, primary_key, t0))	
																														
	private def global_compute(object_name: String,
	                           primary_key: PRIMARY_KEY,
												     table: TABLE):
	TABLE = TableUtil.get_object_desc(object_name, table).fold(table){
		case ENTITY_TX_DESC(_, _, _, global_list, _, _) => global_compute(primary_key, global_list, table)
		case _: INDEX_DESC                              => table
	}
	
	private def update_index(data_map: DATA_MAP,
	                         index_desc: INDEX_DESC,
									         update_flag: Boolean,
									         table: TABLE):
	TABLE = {
	  (update_flag, PrimaryKey.make_primary_key(index_desc.primary_key, data_map)) match {
	  	case (true, (Some(upk), _))  => update_attribute(index_desc.object_name, 
				                                               upk, 
																							         DataModelUtil.attribute_value_list(index_desc.attribute_list, data_map),
																						           table)
			case (true, (_, Some(dpk)))  => delete(index_desc.object_name, index_desc.delete_primary_key.make(dpk), table)
			case (false, (Some(dpk), _)) => delete(index_desc.object_name, dpk, table)
			case _                       => table
	  }
	}
	
	private def update_index(data_map: DATA_MAP,
	                         index_list: List[INDEX_DESC],
													 update_flag: Boolean,
													 table: TABLE):
	TABLE = index_list.foldLeft(table)((t0, id0) => update_index(data_map, id0, update_flag, t0))	

  private def update_index(primary_key: PRIMARY_KEY,
	                         index_list: List[INDEX_DESC],
													 update_flag: Boolean,
													 table: TABLE):
	TABLE = get(primary_key, table).fold(table)(data_map => update_index(data_map, index_list, update_flag, table))

	private def update_dependency(object_name: String,
	                              primary_key: PRIMARY_KEY,
												        table: TABLE):
	TABLE = TableUtil.get_object_desc(object_name, table).fold(table){
		case ENTITY_TX_DESC(_, _, local_list, global_list, index_list, _) => 
		  global_compute(primary_key, 
			               global_list,
										 update_index(primary_key,
										              index_list,
																  true,
										              local_compute(primary_key, 
																	              local_list, 
																	              table)))
		case _: INDEX_DESC                                                => table
	}
	
	private def delete_detail(data_map: DATA_MAP,
	                          entity_tx_desc: ENTITY_TX_DESC,
									          table: TABLE):
	TABLE = {
	  PrimaryKey.make_primary_key(entity_tx_desc.primary_key, data_map) match {
			case (Some(dpk), _) => delete(entity_tx_desc.object_name, dpk, table)
			case _              => table
	  }
	}	
	
	private def delete_detail(data_map_list: List[DATA_MAP],
	                          entity_tx_desc: ENTITY_TX_DESC,
													  table: TABLE):
	TABLE = data_map_list.foldLeft(table)((t0, dmap0) => delete_detail(dmap0, entity_tx_desc, t0))
	
	private def delete_detail(primary_key: PRIMARY_KEY,
	                          entity_tx_desc: ENTITY_TX_DESC,
													  table: TABLE):
  TABLE = delete_detail(get_list(PrimaryKey.make_detail_key(entity_tx_desc.primary_key, primary_key), table), 
                        entity_tx_desc,
											  table)
												
	private def delete_detail(primary_key: PRIMARY_KEY,
	                          entity_tx_desc_list: List[ENTITY_TX_DESC],
													  table: TABLE):
	TABLE = entity_tx_desc_list.foldLeft(table)((t0, e0) => delete_detail(primary_key, e0, t0))
}