package wide_column

final case object DataModel {	
	val node_type         = "node_type"
	val str_attr_list     = "attribute_list"
	val missing_key_value = MISSING_KEY_VALUE()
	
	type DATA_MAP      = Map[String, Any]
	type MAP_ATTR_DATA = Map[Any, Any]

  sealed trait NODE_TYPE
	final case object NODE          extends NODE_TYPE
	final case object LEAF          extends NODE_TYPE
	
  sealed trait DATA_STATUS
	final case object EMPTY_STATUS  extends DATA_STATUS
	final case object EXISTS_STATUS extends DATA_STATUS	
	
	sealed trait ATTRIBUTE_DESC
	sealed trait KEY_DESC
	sealed trait DERIVED_ATTRIBUTE_DESC
		
	sealed trait ATTRIBUTE_VALUE
	sealed trait KEY_VALUE
	
	final case class MapDataStatus(data_status:               DATA_STATUS, 
		                             data_key:                  String,
		                             data_map:                  DATA_MAP)

	final case class QUALIFIED_ATTRIBUTE_DESC(object_name:    String,
															 	            attribute_name: String,
															 							qualified_name: String)
									 extends ATTRIBUTE_DESC
	
  trait CheckFunction {
		def check(qname:    String, 
		          data_map: DATA_MAP): Option[Any]					 	
	}
	
	trait LocalValueFunction {
		def value(args: List[Any]): Any															
	}
	
	trait GlobalValueFunction {
		def value(args: List[DATA_MAP]): Any
	}
	
	trait LocalComputeFunction {
		def compute(src_attribute_list: List[ATTRIBUTE_DESC],
			          trg_attribute_desc: ATTRIBUTE_DESC,
								check_function:     CheckFunction,
								value_function:     LocalValueFunction,
								data_map:           DATA_MAP): DATA_MAP
	}
	
	trait GlobalComputeFunction {
		def compute(src_key_desc:       PRIMARY_KEY_DESC,
		            trg_key_desc:       PRIMARY_KEY_DESC,
							  trg_object_name:    String,
								trg_attribute_desc: ATTRIBUTE_DESC,
							  value_function:     GlobalValueFunction,
								primary_key:        PRIMARY_KEY,
								table:              TABLE): TABLE
	}
	
	trait DeletePrimaryKey {
		def make(primary_key: PRIMARY_KEY): PRIMARY_KEY
	}
																	
	final case class LOCAL_INPUT(attribute_list:              List[ATTRIBUTE_DESC],
		                           check_function:              CheckFunction,
	                             value_function:              LocalValueFunction,
															 compute_function:            LocalComputeFunction)
																	
	final case class LOCAL_ATTRIBUTE_DESC(attribute_desc:     ATTRIBUTE_DESC,
																				compute_input:      LOCAL_INPUT)
								 	 extends DERIVED_ATTRIBUTE_DESC									 

  final case class GLOBAL_INPUT(src_key_desc:               PRIMARY_KEY_DESC,
	                              trg_key_desc:               PRIMARY_KEY_DESC,
	                              trg_object_name:            String,
	                              value_function:             GlobalValueFunction,
																compute_function:           GlobalComputeFunction)
  	
	final case class GLOBAL_ATTRIBUTE_DESC(attribute_desc:    ATTRIBUTE_DESC,
																				 compute_input:     GLOBAL_INPUT)
								 	 extends DERIVED_ATTRIBUTE_DESC	
									 																 
	final case class VALUE_KEY_DESC(value:                    Any)
								 	 extends KEY_DESC
	final case class QUALIFIED_KEY_DESC(object_name:          String,
								 											attribute_name:       String,
								 											qualified_name:       String)
								 	 extends KEY_DESC
									 
 	final case class MAP_ATTRIBUTE_VALUE(attribute:           ATTRIBUTE_DESC,
 								 								 	     value:               MAP_ATTR_DATA)
 								 	 extends ATTRIBUTE_VALUE
 	final case class ANY_ATTRIBUTE_VALUE(attribute:           ATTRIBUTE_DESC,
 								 								 			 value:               Any)
 								 	 extends ATTRIBUTE_VALUE
									 									 								
	final case class MISSING_KEY_VALUE()
								 	 extends KEY_VALUE
	final case class VALUE_KEY_VALUE(value:                   Any)	
								 	 extends KEY_VALUE											
	final case class ATTRIBUTE_KEY_VALUE(attribute:           KEY_DESC,
								 								 			 value:               Any)
								 	 extends KEY_VALUE	
									 
  final case class PRIMARY_KEY_DESC(primary_key:            List[KEY_DESC])
									 
	sealed trait OBJECT_TYPE_DESC
	final case class ENTITY_TX_DESC(object_name:              String,
								 		              primary_key:              PRIMARY_KEY_DESC,
																  local_list:               List[LOCAL_ATTRIBUTE_DESC]  = List.empty[LOCAL_ATTRIBUTE_DESC],
																  global_list:              List[GLOBAL_ATTRIBUTE_DESC] = List.empty[GLOBAL_ATTRIBUTE_DESC],
																  index_list:               List[INDEX_DESC]            = List.empty[INDEX_DESC],
																  detail_list:              List[ENTITY_TX_DESC]        = List.empty[ENTITY_TX_DESC])
								 	 extends OBJECT_TYPE_DESC
	final case class INDEX_DESC(object_name:                  String,
								 	            primary_key:                  PRIMARY_KEY_DESC,
								 							attribute_list:               List[ATTRIBUTE_DESC],
														  delete_primary_key:           DeletePrimaryKey)
								 	 extends OBJECT_TYPE_DESC
									  																					 	 
	final case class TABLE_DESC(table_name:                   String,
								 							object_type:                  Map[String, OBJECT_TYPE_DESC])	
															
	final case class PRIMARY_KEY(primary_key:                 List[KEY_VALUE])
															 
	final case class TABLE(table_desc:                        TABLE_DESC,
												 table_data:                        DATA_MAP)									 								
	
}