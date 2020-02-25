package wide_column

import DataModel._

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

	val product_desc = Attribute.make_attribute_desc(product_object_name, "product_desc")
	val vendor_name = Attribute.make_attribute_desc(vendor_object_name, "vendor_name")
  val delivery_dt = Attribute.make_attribute_desc(order_detail_object_name, "delivery_dt")
	val vendor_id = Attribute.make_attribute_desc(vendor_object_name, "vendor_id")
	val key_vendor_id = Attribute.make_key_desc(vendor_object_name, "vendor_id")
	val key_open_status = Attribute.make_key_desc(order_detail_object_name, "open_status")
	val order_id = Attribute.make_attribute_desc(order_object_name, "order_id")
	val key_order_id = Attribute.make_key_desc(order_object_name, "order_id")
	val product_id = Attribute.make_attribute_desc(product_object_name, "product_id")
	val key_product_id = Attribute.make_key_desc(product_object_name, "product_id")
	val price = Attribute.make_attribute_desc(order_detail_object_name, "price")	
  val quantity = Attribute.make_attribute_desc(order_detail_object_name, "quantity")
  val detail_status = Attribute.make_attribute_desc(order_detail_object_name, "detail_status")
  val order_dt = Attribute.make_attribute_desc(order_object_name, "order_dt")
	val customer_id = Attribute.make_attribute_desc(customer_object_name, "customer_id")

	val key_order = VALUE_KEY_DESC(order_object_name)
	val key_order_detail = VALUE_KEY_DESC(order_detail_object_name)
	val key_open_order = VALUE_KEY_DESC(open_order_object_name)
	
	val src_order_detail_pk = PRIMARY_KEY_DESC(List(key_order_id, key_order_detail))
	val order_pk = PRIMARY_KEY_DESC(List(key_order_id, key_order))																 
	val open_order_pk = PRIMARY_KEY_DESC(List(key_vendor_id, key_open_order, key_open_status, key_order_id, key_product_id))
	val order_detail_pk = PRIMARY_KEY_DESC(List(key_order_id, key_order_detail, key_vendor_id, key_product_id))															 
}

final case object OpenOrderDeletePrimaryKey extends DeletePrimaryKey {
  def make(primary_key: PRIMARY_KEY):
	PRIMARY_KEY = PRIMARY_KEY(primary_key.primary_key.map{
		case DataModel.missing_key_value => ATTRIBUTE_KEY_VALUE(SchemaConstants.key_open_status, 
			                                                      SchemaConstants.detail_status_open)
		case akv                         => akv
	})	
}

final case object AggregateOrderDetailAmount extends GlobalValueFunction {
	def value(args: List[DATA_MAP]): 
	Any = args.foldLeft(0.0)((v0, dmap0) => dmap0.get(Attribute.get_qualified_name(SchemaConstants.detail_status)).fold(v0){
		case SchemaConstants.detail_status_cancelled => v0
		case _                                       => dmap0.get(Attribute.get_qualified_name(Schema.amount)).
		                                                  fold(v0)(v1 => v0+v1.asInstanceOf[Double])
	})	
}

final case object OpenStatusCheckFunction extends CheckFunction {
	def check(key: String, map: DATA_MAP):
	Option[Any] = map.get(key).fold(None: Option[Any])(value => value match {
		case SchemaConstants.detail_status_open => Some(value)
		case _                                  => None: Option[Any]
	})
}

final case object Schema {
	val open_status = Attribute.make_attribute_desc(SchemaConstants.order_detail_object_name, 
		                                              "open_status",
																									LOCAL_INPUT(List(SchemaConstants.detail_status),
																															OpenStatusCheckFunction,
																															IdentityUnaryFunction,
																															UnaryFunction))
																																							
	val amount = Attribute.make_attribute_desc(SchemaConstants.order_detail_object_name, 
		                                         "amount",
																						 LOCAL_INPUT(List(SchemaConstants.quantity, SchemaConstants.price),
	 																											 CheckDataMap,
																												 IDDProductFunction,
																												 BinaryFunction))
																																					
	val order_amount = Attribute.make_attribute_desc(SchemaConstants.order_object_name,
	                                                 "order_amount",
																									 GLOBAL_INPUT(SchemaConstants.src_order_detail_pk,
																												        SchemaConstants.order_pk,
																																SchemaConstants.order_object_name,
																																AggregateOrderDetailAmount,
																																ComputeAggregateFunction))
																																					
	val open_order = INDEX_DESC(SchemaConstants.open_order_object_name, 
															SchemaConstants.open_order_pk,
															List(SchemaConstants.order_id, 
																	 SchemaConstants.vendor_id, 
																	 SchemaConstants.vendor_name, 
																	 SchemaConstants.product_id, 
																	 SchemaConstants.product_desc, 
																	 SchemaConstants.quantity, 
																	 SchemaConstants.delivery_dt),
															OpenOrderDeletePrimaryKey)
															
	val order = ENTITY_TX_DESC(SchemaConstants.order_object_name, 
		                         SchemaConstants.order_pk)

	val order_detail = ENTITY_TX_DESC(SchemaConstants.order_detail_object_name, 
		                                SchemaConstants.order_detail_pk,
																	  List(amount, open_status),
																	  List(order_amount),
																	  List(open_order))
	
	val table_desc = Table.create_table_desc(SchemaConstants.schema_name,
																 	         List(order, order_detail, open_order))
}

final case object SchemaInterface {
	def create_table():
	TABLE = Table.create_table(Schema.table_desc)
	
	def open_order_query_key(vid: Int):
	PRIMARY_KEY = PRIMARY_KEY(List(ATTRIBUTE_KEY_VALUE(SchemaConstants.key_vendor_id, vid), 
		                             VALUE_KEY_VALUE(SchemaConstants.open_order_object_name)))
																 
	def order_primary_key(oid: Int):
	PRIMARY_KEY = PRIMARY_KEY(List(ATTRIBUTE_KEY_VALUE(SchemaConstants.key_order_id, oid), 
		                             VALUE_KEY_VALUE(SchemaConstants.order_object_name)))
																 
	def order_attribute_list(oid: Int, cid: Int, odt: String):
	List[ATTRIBUTE_VALUE] = List(ANY_ATTRIBUTE_VALUE(SchemaConstants.order_id, oid),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.customer_id, cid),
															 ANY_ATTRIBUTE_VALUE(Schema.order_amount.attribute_desc, 0.0),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.order_dt, odt))
															 
	def update_order(oid: Int, cid: Int, odt: String, table: TABLE):
	TABLE = Table.update_attribute(SchemaConstants.order_object_name,
	                               order_primary_key(oid),
															   order_attribute_list(oid, cid, odt),
															   table)
																 
	def order_detail_primary_key(oid: Int, vid: Int, pid: Int):
	PRIMARY_KEY = PRIMARY_KEY(List(ATTRIBUTE_KEY_VALUE(SchemaConstants.key_order_id, oid),
															 	 VALUE_KEY_VALUE(SchemaConstants.order_detail_object_name),
															 	 ATTRIBUTE_KEY_VALUE(SchemaConstants.key_vendor_id, vid),
															 	 ATTRIBUTE_KEY_VALUE(SchemaConstants.key_product_id, pid)))
										 
	def order_detail_attribute_list(oid: Int, vid: Int, pid: Int, pds: String, vnm: String, 
															 		prc: Double, qty: Int, ddt: String):
	List[ATTRIBUTE_VALUE] = List(ANY_ATTRIBUTE_VALUE(SchemaConstants.order_id, oid),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.vendor_id, vid),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.product_id, pid),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.product_desc, pds),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.vendor_name, vnm),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.price, prc),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.quantity, qty),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.detail_status, SchemaConstants.detail_status_open),
															 ANY_ATTRIBUTE_VALUE(SchemaConstants.delivery_dt, ddt))
															 
	def update_order_detail(oid: Int, vid: Int, pid: Int, pds: String, vnm: String, 
													prc: Double, qty: Int, ddt: String, table: TABLE):
	TABLE = Table.update_attribute(SchemaConstants.order_detail_object_name,
	                               order_detail_primary_key(oid, vid, pid),
															   order_detail_attribute_list(oid, vid, pid, pds, vnm, prc, qty, ddt),
															   table)
																 
	def update_order_detail_status(oid: Int, vid: Int, pid: Int, dts: String, table: TABLE):
	TABLE = Table.update_attribute(SchemaConstants.order_detail_object_name,
	                               order_detail_primary_key(oid, vid, pid),
															   List(ANY_ATTRIBUTE_VALUE(SchemaConstants.detail_status, dts)),
															   table)

	def update_order_detail_quantity(oid: Int, vid: Int, pid: Int, qty: Int, table: TABLE):
	TABLE = Table.update_attribute(SchemaConstants.order_detail_object_name,
															 	 order_detail_primary_key(oid, vid, pid),
															 	 List(ANY_ATTRIBUTE_VALUE(SchemaConstants.quantity, qty)),
															 	 table)
																 
	def delete_order_detail(oid: Int, vid: Int, pid: Int, table: TABLE):
	TABLE = Table.delete(SchemaConstants.order_detail_object_name,
											 order_detail_primary_key(oid, vid, pid),
											 table)
}

final case object SchemaTest {
	def make_table():
	TABLE = {
		val t0 = SchemaInterface.create_table()
		val t1 = SchemaInterface.update_order(1, 50, "2020-02-25", t0)
		val t2 = SchemaInterface.update_order_detail(1, 10, 100, "Avocado Oil", "Chosen Food", 19.0, 2, "2020-02-29", t1)
		val t3 = SchemaInterface.update_order_detail(1, 10, 300, "Olive Oil", "Chosen Food", 5.0, 4, "2020-02-29", t2)
		SchemaInterface.update_order_detail(1, 20, 200, "Almonds", "nuts.com", 11.0, 3, "2020-02-29", t3)
	}
}