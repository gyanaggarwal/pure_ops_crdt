package wide_column

import DataModel._

final case object UnaryFunction extends LocalComputeFunction {
	def compute(attribute_list: List[ATTRIBUTE_DESC],
	            maps:           List[DATA_MAP],
						  value_function: ValueFunction):
	Option[Any] = (attribute_list, maps) match {
		case ((ad0 :: _), (map0 :: _)) => for {
			v0 <- map0.get(Attribute.get_qualified_name(ad0))
		} yield value_function.value(List(v0))
		case _                         => None: Option[Any]
	}
}

final case object BinaryFunction1 extends LocalComputeFunction {
	def compute(attribute_list: List[ATTRIBUTE_DESC],
	            maps:           List[DATA_MAP],
						  value_function: ValueFunction):
	Option[Any] = (attribute_list, maps) match {
		case ((ad0 :: ad1 :: _), (map0 :: _)) => for {
			v0 <- map0.get(Attribute.get_qualified_name(ad0))
			v1 <- map0.get(Attribute.get_qualified_name(ad1))
		} yield value_function.value(List(v0, v1))
		case _                                => None: Option[Any]
	}
}

final case object BinaryFunction2 extends LocalComputeFunction {
	def compute(attribute_list: List[ATTRIBUTE_DESC],
	            maps:           List[DATA_MAP],
						  value_function: ValueFunction):
	Option[Any] = (attribute_list, maps) match {
		case ((ad0 :: ad1 :: _), (map0 :: map1 :: _)) => for {
			v0 <- map0.get(Attribute.get_qualified_name(ad0))
			v1 <- map1.get(Attribute.get_qualified_name(ad1))
		} yield value_function.value(List(v0, v1))
		case _                                        => None: Option[Any]
	}
}

final case object ComputeAggregateFunction extends GlobalAggregateFunction {
  def compute(attribute_desc: GAGGREGATE_ATTRIBUTE_DESC,
			        primary_key:    PRIMARY_KEY,
							table:          TABLE): 
	TABLE = {
		val input = Attribute.get_gaggregate_input(attribute_desc)
		(for {
			spk <- PrimaryKey.make_primary_key(input.src_key_desc, primary_key)
			dmp <- Table.get(spk, table)
		} yield dmp).fold(table)(dmap => {
			PrimaryKey.make_primary_key(input.trg_key_desc, primary_key).fold(table)(tpk => {
				Table.update_attribute(input.trg_object_name,
				                       tpk,
														   List(ANY_ATTRIBUTE_VALUE(attribute_desc,
															                          input.aggregate_value_function.value(DataModelUtil.collect_leaf(dmap)))),
															 table)
			})
		})
	}	
}

final case object IDDProductFunction extends ValueFunction {
	def value(args: List[Any]):
	Any = args match {
		case (v0 :: v1 :: _) => v0.asInstanceOf[Int]*v1.asInstanceOf[Double]
		case _               => 0.0
	}
}

final case object DDDSumFunction extends ValueFunction {
	def value(args: List[Any]):
	Any = args match {
		case (v0 :: v1 :: _) => v0.asInstanceOf[Double]+v1.asInstanceOf[Double]
		case _               => 0.0
	}
}

final case object IdentityUnaryFunction extends ValueFunction {
	def value(args: List[Any]):
	Any = args match {
		case (v0 :: _) => v0
		case _         => ()
	}
}

final case object CheckFunction {
	def compute(attribute_list: List[ATTRIBUTE_DESC],
	            maps:           List[DATA_MAP],
						  value_function: ValueFunction,
						  check_function: (String, DATA_MAP) => Option[Any]):
	Option[Any] = (attribute_list, maps) match {
		case ((ad0 :: _), (map0 :: _)) => for {
			v0 <- check_function(Attribute.get_qualified_name(ad0), map0)
		} yield value_function.value(List(v0))
		case _                         => None: Option[Any]
	}
}