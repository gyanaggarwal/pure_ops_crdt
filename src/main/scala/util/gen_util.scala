package util

object GenUtil {
	def take_map[K, V](key: K, map: Map[K, V]):
	(Map[K, V], Option[V]) = (map - key, map.get(key))
	
	def remove_from_list[NODE_ID](node_id: NODE_ID,
	                              node_list: List[NODE_ID],
															  anyId: AnyId[NODE_ID]):
	List[NODE_ID] = node_list.filter(!anyId.eqv(_, node_id))
}