package wide_column

final case object DataModel {
	val node_type = "node_type"
	val empty_key_value = MISSING_ATTRIBUTE_VALUE()
	
	type DATA_MAP      = Map[String, Any]
	type MAP_ATTR_DATA = Map[Any, Any]

  sealed trait NODE_TYPE
	final case object NODE extends NODE_TYPE
	final case object LEAF extends NODE_TYPE
	
  sealed trait DATA_STATUS
	final case object EMPTY_STATUS  extends DATA_STATUS
	final case object EXISTS_STATUS extends DATA_STATUS
	
	final case class MapDataStatus(data_status: DATA_STATUS, 
		                             data_key:    String,
		                             data_map:    DATA_MAP)
	
	sealed trait ATTRIBUTE_DESC
	sealed trait COMPUTED_DESC             extends ATTRIBUTE_DESC
	sealed trait ATTRIBUTE_KEY_DESC        extends ATTRIBUTE_DESC
	sealed trait LCOMPUTE_ATTRIBUTE_DESC   extends COMPUTED_DESC
	sealed trait GAGGREGATE_ATTRIBUTE_DESC extends COMPUTED_DESC
	
	final case class QUALIFIED_ATTRIBUTE_DESC(object_name:    String,
	                                          attribute_name: String,
																					  qualified_name: String)
									 extends ATTRIBUTE_KEY_DESC
									 
	trait ValueFunction {
		def value(args: List[Any]): Any															
	}
	
	trait LocalComputeFunction {
		def compute(attribute_list: List[ATTRIBUTE_DESC],
								maps:           List[DATA_MAP],
								value_function: ValueFunction): Option[Any]
	}
	
	trait AggregateValueFunction {
		def value(args: List[DATA_MAP]): Any
	}
	
	trait GlobalAggregateFunction {
		def compute(atribute_desc: GAGGREGATE_ATTRIBUTE_DESC,
			          primary_key:   PRIMARY_KEY,
							  table:         TABLE): TABLE
	}
	
	trait DeletePrimaryKey {
		def make(primary_key: PRIMARY_KEY): PRIMARY_KEY
	}
	
	final case class LCOMPUTE_INPUT(attribute_list:   List[ATTRIBUTE_DESC],
	                                value_function:   ValueFunction,
																	compute_function: LocalComputeFunction)
																	
	final case class LCOMPUTE_QUALIFIED_ATTRIBUTE_DESC(qualified_desc: QUALIFIED_ATTRIBUTE_DESC,
																										 lcompute_input: LCOMPUTE_INPUT)
								 	 extends LCOMPUTE_ATTRIBUTE_DESC									 

  final case class GAGGREGATE_INPUT(src_key_desc:              PRIMARY_KEY_DESC,
	                                  trg_object_name:           String,
	                                  trg_key_desc:              PRIMARY_KEY_DESC,
	                                  aggregate_value_function:  AggregateValueFunction,
																	  global_aggregate_function: GlobalAggregateFunction)
  	
	final case class GAGGREGATE_QUALIFIED_ATTRIBUTE_DESC(qualified_desc: QUALIFIED_ATTRIBUTE_DESC,
																										   gaggregate_input: GAGGREGATE_INPUT)
								 	 extends GAGGREGATE_ATTRIBUTE_DESC									 
									 
	final case class PRIMARY_KEY_DESC(primary_key:  List[ATTRIBUTE_KEY_DESC])
									 
	sealed trait OBJECT_TYPE_DESC
	final case class ENTITY_TX_DESC(object_name:   String,
		                              primary_key:   PRIMARY_KEY_DESC,
																  lcompute_list: List[LCOMPUTE_ATTRIBUTE_DESC] = List.empty[LCOMPUTE_ATTRIBUTE_DESC],
																  gcompute_list: List[GAGGREGATE_ATTRIBUTE_DESC] = List.empty[GAGGREGATE_ATTRIBUTE_DESC],
																  index_list:    List[INDEX_DESC] = List.empty[INDEX_DESC])
									 extends OBJECT_TYPE_DESC
	final case class INDEX_DESC(object_name:        String,
	                            primary_key:        PRIMARY_KEY_DESC,
														  attribute_list:     List[ATTRIBUTE_DESC],
														  delete_primary_key: DeletePrimaryKey)
									 extends OBJECT_TYPE_DESC
									  																					 	 
	final case class TABLE_DESC(table_name:  String,
								 	            object_type: Map[String, OBJECT_TYPE_DESC])
	
	sealed trait ATTRIBUTE_VALUE
	final case class MAP_ATTRIBUTE_VALUE(attribute:   ATTRIBUTE_DESC,
								 	                     value:       MAP_ATTR_DATA)
								 	 extends ATTRIBUTE_VALUE
	final case class ANY_ATTRIBUTE_VALUE(attribute:   ATTRIBUTE_DESC,
								 								 	     value:       Any)
								 	 extends ATTRIBUTE_VALUE									 								
	
	sealed trait ATTRIBUTE_KEY_VALUE extends ATTRIBUTE_VALUE	
	final case class VALUE_ATTRIBUTE(value: Any)	
	                 extends ATTRIBUTE_KEY_DESC with ATTRIBUTE_KEY_VALUE											
	final case class MISSING_ATTRIBUTE_VALUE(value:   Any = ())
								 	 extends ATTRIBUTE_KEY_VALUE
	final case class KEY_ATTRIBUTE_VALUE(attribute:   ATTRIBUTE_DESC,
								 								 			 value:       Any)
								 	 extends ATTRIBUTE_KEY_VALUE									 								
									 
	final case class PRIMARY_KEY(primary_key: List[ATTRIBUTE_KEY_VALUE])
															 
	final case class TABLE(table_desc: TABLE_DESC,
	                       table_data: DATA_MAP)												
}