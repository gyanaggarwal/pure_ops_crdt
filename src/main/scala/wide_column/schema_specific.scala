package wide_column

import DataModel._

import scala.collection.immutable._

final case object SchemaConstants {
	val vendor_object_name = "VENDOR"
	val product_object_name = "PRODUCT"
	val order_object_name = "ORDER"
	val order_detail_object_name = "ORDER_DETAIL"
	val open_order_object_name = "OPEN_ORDER"
	val customer_object_name = "CUSTOMER"
	
	val schema_name = "ONLINE_MARKET"
	
	val detail_status_open = "OPEN"
	val detail_status_shipped = "SHIPPED"
	val detail_status_cancelled = "CANCELLED"

	val product_desc = Attribute.make_qualified_desc(product_object_name, "product_desc")
	val vendor_name = Attribute.make_qualified_desc(vendor_object_name, "vendor_name")
  val delivery_dt = Attribute.make_qualified_desc(order_detail_object_name, "delivery_dt")
	val vendor_id = Attribute.make_qualified_desc(vendor_object_name, "vendor_id")
	val key_open_status = Attribute.make_qualified_desc(order_detail_object_name, "open_status")
	val order_id = Attribute.make_qualified_desc(order_object_name, "order_id")
	val product_id = Attribute.make_qualified_desc(product_object_name, "product_id")
	val price = Attribute.make_qualified_desc(order_detail_object_name, "price")	
  val quantity = Attribute.make_qualified_desc(order_detail_object_name, "quantity")
  val detail_status = Attribute.make_qualified_desc(order_detail_object_name, "detail_status")
	val mailing_address = Attribute.make_qualified_desc(customer_object_name,"mailing_address")
  val order_dt = Attribute.make_qualified_desc(order_object_name, "order_dt")
  val order_amount = Attribute.make_qualified_desc(order_object_name, "order_amount")
	val customer_id = Attribute.make_qualified_desc(customer_object_name, "customer_id")
	
	val todopk = PRIMARY_KEY_DESC(List(order_id, 
																	   VALUE_ATTRIBUTE(order_detail_object_name)))

	val topk = PRIMARY_KEY_DESC(List(order_id, 
		                               VALUE_ATTRIBUTE(order_object_name)))
	
	val g_input = GAGGREGATE_INPUT(todopk, order_object_name, topk, AggregateOrderDetailAmount, ComputeAggregateFunction)
																	 
  val amount = Attribute.make_qualified_desc(order_detail_object_name, 
		                                         "amount", 
																						 LCOMPUTE_INPUT(List(quantity, price), 
																											      IDDProductFunction, 
																											      BinaryFunction1))	
	val open_status = Attribute.make_qualified_desc(order_detail_object_name, 
																									"open_status",
																									LCOMPUTE_INPUT(List(detail_status),
																													       IdentityUnaryFunction,
																													       OpenStatusFunction))
	
	val compute_order_amount = Attribute.make_qualified_desc(order_object_name, 
		                                                       "order_amount",
																													 g_input)
																																																														 
	val detail_status_col = Attribute.get_qualified_name(detail_status)
	val amount_col = Attribute.get_qualified_name(amount)
}

final case object OpenStatusFunction extends LocalComputeFunction {
	def check_function(key: String, map: DATA_MAP):
	Option[Any] = map.get(key).fold(None: Option[Any])(value => value match {
		case SchemaConstants.detail_status_open => Some(value)
		case _                                  => None: Option[Any]
	})
	
	def compute(attribute_list: List[ATTRIBUTE_DESC],
		          maps:           List[DATA_MAP],
							value_function: ValueFunction):
	Option[Any] = CheckFunction.compute(attribute_list, maps, value_function, check_function)
}

final case object AggregateOrderDetailAmount extends AggregateValueFunction {
	def value(args: List[DATA_MAP]): 
	Any = args.foldLeft(0.0)((v0, dmap0) => dmap0.get(SchemaConstants.detail_status_col).fold(v0){
		case SchemaConstants.detail_status_cancelled => v0
		case _                                       => dmap0.get(SchemaConstants.amount_col).
		                                                  fold(v0)(v1 => v0+v1.asInstanceOf[Double])
	})	
}

final case object OpenOrderDeletePrimaryKey extends DeletePrimaryKey {
  def make(primary_key: PRIMARY_KEY):
	PRIMARY_KEY = PRIMARY_KEY(primary_key.primary_key.map{
		case _: MISSING_ATTRIBUTE_VALUE => KEY_ATTRIBUTE_VALUE(SchemaConstants.key_open_status, SchemaConstants.detail_status_open)
		case akv                        => akv
	})	
}

final case object Schema {
	val ioopk = PRIMARY_KEY_DESC(List(SchemaConstants.vendor_id, 
		                                VALUE_ATTRIBUTE(SchemaConstants.open_order_object_name),
																		SchemaConstants.key_open_status, 
																		SchemaConstants.order_id, 
																		SchemaConstants.product_id))
																																			 
	val open_order = INDEX_DESC(SchemaConstants.open_order_object_name,
	                            ioopk,
														  List(SchemaConstants.order_id, 
																   SchemaConstants.vendor_id, 
																	 SchemaConstants.vendor_name, 
																	 SchemaConstants.product_id, 
																	 SchemaConstants.product_desc, 
																	 SchemaConstants.quantity, 
																	 SchemaConstants.mailing_address,
																	 SchemaConstants.delivery_dt),
															OpenOrderDeletePrimaryKey) 
																	 
	val todpk = PRIMARY_KEY_DESC(List(SchemaConstants.order_id, 
		                                VALUE_ATTRIBUTE(SchemaConstants.order_detail_object_name), 
																	  SchemaConstants.product_id, 
																	  SchemaConstants.vendor_id))
																																											
	val order_detail = ENTITY_TX_DESC(SchemaConstants.order_detail_object_name, 
		                                todpk, 
																		List(SchemaConstants.amount, 
																			   SchemaConstants.open_status),
																		List(SchemaConstants.compute_order_amount),
																	  List(open_order))
	
	val order = ENTITY_TX_DESC(SchemaConstants.order_object_name, 
		                         SchemaConstants.topk)
													 
	val table_desc = Table.create_table_desc(SchemaConstants.schema_name,
	                                         List(order, 
																						    order_detail, 
																								open_order))
															
	val table = Table.create_table(table_desc)
	
	val maddress = MAP_ATTRIBUTE_VALUE(SchemaConstants.mailing_address, 
		                                 HashMap("home" -> HashMap("full_name" -> "John Doe",
                                                               "street_address" -> "100 Hope Street",
																													      "city" -> "Stamford",
																													      "state" -> "CT",
																													      "zip_code" -> "06906")))

	def order_detail_order_key(oid: Int) =
	  PRIMARY_KEY(List(KEY_ATTRIBUTE_VALUE(SchemaConstants.order_id, oid),
		                 VALUE_ATTRIBUTE(SchemaConstants.order_detail_object_name)))
				
	def order_detail_primary_key(oid: Int, vid: Int, pid: Int) =
	  PRIMARY_KEY(List(KEY_ATTRIBUTE_VALUE(SchemaConstants.order_id, oid),
		                 VALUE_ATTRIBUTE(SchemaConstants.order_detail_object_name),
	                   KEY_ATTRIBUTE_VALUE(SchemaConstants.vendor_id, vid),
									   KEY_ATTRIBUTE_VALUE(SchemaConstants.product_id, pid)))
										 
	def order_detail_attribute_list(oid: Int, vid: Int, pid: Int, pds: String, vnm: String, 
		                              prc: Double, qty: Int, ddt: String) =
	  List(ANY_ATTRIBUTE_VALUE(SchemaConstants.order_id, oid),
	       ANY_ATTRIBUTE_VALUE(SchemaConstants.vendor_id, vid),
				 ANY_ATTRIBUTE_VALUE(SchemaConstants.product_id, pid),
			   ANY_ATTRIBUTE_VALUE(SchemaConstants.product_desc, pds),
			   ANY_ATTRIBUTE_VALUE(SchemaConstants.vendor_name, vnm),
			   ANY_ATTRIBUTE_VALUE(SchemaConstants.price, prc),
			   ANY_ATTRIBUTE_VALUE(SchemaConstants.quantity, qty),
			   ANY_ATTRIBUTE_VALUE(SchemaConstants.detail_status, SchemaConstants.detail_status_open),
			   ANY_ATTRIBUTE_VALUE(SchemaConstants.delivery_dt, ddt))
			 
	def order_primary_key(oid: Int) =
		PRIMARY_KEY(List(KEY_ATTRIBUTE_VALUE(SchemaConstants.order_id, oid), 
		                 VALUE_ATTRIBUTE(SchemaConstants.order_object_name)))
										 
	def order_attribute_list(oid: Int, cid: Int, odt: String) =
		List(ANY_ATTRIBUTE_VALUE(SchemaConstants.order_id, oid),
		     ANY_ATTRIBUTE_VALUE(SchemaConstants.customer_id, cid),
				 ANY_ATTRIBUTE_VALUE(SchemaConstants.order_amount, 0.0),
				 ANY_ATTRIBUTE_VALUE(SchemaConstants.order_dt, odt),
			   maddress)
	
	def open_order_primary_key(vid: Int) = 
	  PRIMARY_KEY(List(KEY_ATTRIBUTE_VALUE(SchemaConstants.vendor_id, vid), 
	                   VALUE_ATTRIBUTE(SchemaConstants.open_order_object_name)))	
										 		 
	def make_table() = {
		val pko1 = order_primary_key(1)
		val alo0 = order_attribute_list(1, 50, "2020-02-25")
		
		val pkod111 = order_detail_primary_key(1, 10, 100)
		val alod1 = order_detail_attribute_list(1, 10, 100, "Avocado Oil", "Chosen Food", 19.0, 2, "2020-02-29")
		
		val pkod122 = order_detail_primary_key(1, 20, 200)
		val alod2 = order_detail_attribute_list(1, 20, 200, "Almonds", "nuts.com", 11.0, 3, "2020-02-28")
		
		val t1 = Table.update_attribute(SchemaConstants.order_object_name, pko1, alo0, table)
		val t2 = Table.update_attribute(SchemaConstants.order_detail_object_name, pkod111, alod1, t1)
		Table.update_attribute(SchemaConstants.order_detail_object_name, pkod122, alod2, t2)
	}
	
	def update_quantity(primary_key: PRIMARY_KEY, qty: Int, table: TABLE) =
	  Table.update_attribute(SchemaConstants.order_detail_object_name, 
		                       primary_key, 
												   List(ANY_ATTRIBUTE_VALUE(SchemaConstants.quantity, qty)),
											     table)
												 
	def update_status(primary_key: PRIMARY_KEY, status: String, table: TABLE) =
	  Table.update_attribute(SchemaConstants.order_detail_object_name, 
											 		 primary_key, 
											 		 List(ANY_ATTRIBUTE_VALUE(SchemaConstants.detail_status, status)),
											 		 table)
													 
	def delete_order_detail(primary_key: PRIMARY_KEY, table: TABLE) = 
	  Table.delete(SchemaConstants.order_detail_object_name,
		             primary_key,
							   table)
}

