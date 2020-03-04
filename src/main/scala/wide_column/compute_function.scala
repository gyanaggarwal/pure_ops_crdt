package wide_column

import DataModel._

final case object UnaryFunction extends LocalComputeFunction {
	def compute(src_attribute_list: List[ATTRIBUTE_DESC],
		          trg_attribute_desc: ATTRIBUTE_DESC,
							check_function:     CheckFunction,
		          value_function:     LocalValueFunction,
	            data_map:           DATA_MAP):
	DATA_MAP = {
		val tqname = Attribute.get_qualified_name(trg_attribute_desc)
		(src_attribute_list match {
		  case (ad0 :: _) => for {
			  v0 <- check_function.check(Attribute.get_qualified_name(ad0), data_map)
		  } yield value_function.value(List(v0))
		  case _          => None: Option[Any]
	  }).fold(data_map - tqname)(value => data_map.updated(tqname, value))
	}
}

final case object BinaryFunction extends LocalComputeFunction {
	def compute(src_attribute_list: List[ATTRIBUTE_DESC],
		          trg_attribute_desc: ATTRIBUTE_DESC,
							check_function:     CheckFunction,
		          value_function:     LocalValueFunction,
	            data_map:           DATA_MAP):
	DATA_MAP = {
		val tqname = Attribute.get_qualified_name(trg_attribute_desc)
		(src_attribute_list match {
		  case (ad0 :: ad1 :: _) => for {
			  v0 <- check_function.check(Attribute.get_qualified_name(ad0), data_map)
			  v1 <- check_function.check(Attribute.get_qualified_name(ad1), data_map)
		  } yield value_function.value(List(v0, v1))
		  case _                 => None: Option[Any]
	  }).fold(data_map - tqname)(value => data_map.updated(tqname, value))
	}
}

final case object ComputeAggregateFunction extends GlobalComputeFunction {
  def compute(src_key_desc:       PRIMARY_KEY_DESC,
		          trg_key_desc:       PRIMARY_KEY_DESC,
							trg_object_name:    String,
							trg_attribute_desc: ATTRIBUTE_DESC,
							value_function:     GlobalValueFunction,
			        primary_key:        PRIMARY_KEY,
							table:              TABLE): 
	TABLE = (for {
		tpk <- PrimaryKey.make_primary_key(trg_key_desc, primary_key)
		spk <- PrimaryKey.make_primary_key(src_key_desc, primary_key)
		dmp <- Table.get(spk, table)
	} yield (tpk, dmp)).fold(table){
		case (tpk0, dmp0) => Table.update_attribute(trg_object_name,
		                                            tpk0,
																							  List(ANY_ATTRIBUTE_VALUE(trg_attribute_desc,
																								     value_function.value(DataModelUtil.collect_leaf(dmp0)))),
																								table)
	}
}

final case object IDDProductFunction extends LocalValueFunction {
	def value(args: List[Any]):
	Any = args match {
		case (v0 :: v1 :: _) => v0.asInstanceOf[Int]*v1.asInstanceOf[Double]
		case _               => 0.0
	}
}

final case object DDDSumFunction extends LocalValueFunction {
	def value(args: List[Any]):
	Any = args match {
		case (v0 :: v1 :: _) => v0.asInstanceOf[Double]+v1.asInstanceOf[Double]
		case _               => 0.0
	}
}

final case object IdentityUnaryFunction extends LocalValueFunction {
	def value(args: List[Any]):
	Any = args match {
		case (v0 :: _) => v0
		case _         => ()
	}
}

final case object CheckDataMap extends CheckFunction {
	def check(qualified_name: String,
	          data_map:       DATA_MAP):
	Option[Any] = data_map.get(qualified_name)
}