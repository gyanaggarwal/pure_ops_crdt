package wide_column

import DataModel._

import scala.collection.immutable._

final case object DataModelUtil {
	val empty_map: DATA_MAP                                  = HashMap.empty[String, Any]
	val empty_object_type_map: Map[String, OBJECT_TYPE_DESC] = HashMap.empty[String, OBJECT_TYPE_DESC]
	
	def is_empty(data_map: DATA_MAP):
	Boolean = (data_map - DataModel.node_type).size == 0
	
	def qualified_value(name:  String, 
		                  value: Any):
	String = name+"#"+value.toString
	
	def get(key: String, 
		      map: DATA_MAP):
	MapDataStatus = map.get(key).fold(MapDataStatus(EMPTY_STATUS, key, empty_map))(value =>
			                              MapDataStatus(EXISTS_STATUS, key, value.asInstanceOf[DATA_MAP]))

  def collect_leaf(dmap: DATA_MAP):
	List[DATA_MAP] = dmap.get(DataModel.node_type).
	  fold(List.empty[DATA_MAP]){
			case DataModel.NODE => collect_leaf(dmap, List.empty[DATA_MAP])
			case DataModel.LEAF => List(dmap)
	}
							
	private def collect_leaf(dmap: DATA_MAP, 
		                       dlist: List[DATA_MAP]):
	List[DATA_MAP] = dmap.foldLeft(dlist){
		case (l0, (k0, m0)) => k0 match {
			case DataModel.node_type => l0
			case _                   => {
				val dm0 = m0.asInstanceOf[DATA_MAP]
				dm0.get(DataModel.node_type).fold(l0){
					case DataModel.NODE => collect_leaf(dm0, l0)
					case DataModel.LEAF => dm0 :: l0
	}}}}
	
	def find(key_desc: KEY_DESC,
	         key_list: List[KEY_VALUE]):
	Option[KEY_VALUE] = 
	  key_list.find(kv => Attribute.get_qualified_name(key_desc) == Attribute.get_qualified_name(kv)) 
		
	def attribute_value_list(attribute_list: List[ATTRIBUTE_DESC],
	                         data_map: DATA_MAP):
	List[ATTRIBUTE_VALUE] = attribute_list.foldLeft(List.empty[ATTRIBUTE_VALUE])((l0, ad0) =>
		                        data_map.get(Attribute.get_qualified_name(ad0)).
		                          fold(l0)(value => ANY_ATTRIBUTE_VALUE(ad0, value) :: l0))
}