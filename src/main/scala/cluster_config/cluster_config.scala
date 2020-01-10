package cluster_config

import model.Model._
import vector_clock._
import util._

trait COMPCC
final case object EQCC extends COMPCC
final case object LTCC extends COMPCC
final case object GTCC extends COMPCC
final case object IVCC extends COMPCC

trait ClusterConfig[NODE_ID, CLUSTER_ID] {
  def makeADD(cluster_id: CLUSTER_ID,
	      ver_num: CLUSTER_VER_NUM,
	      node_id: NODE_ID,
	      node_list: List[NODE_ID]):
  CLUSTER_DETAIL[NODE_ID, CLUSTER_ID] = ClusterDetailADD(cluster_id, ver_num, node_id, node_list)
	
  def makeRMV(cluster_id: CLUSTER_ID,
	      ver_num: CLUSTER_VER_NUM,
	      node_id: NODE_ID,
	      node_list: List[NODE_ID]):
  CLUSTER_DETAIL[NODE_ID, CLUSTER_ID] = ClusterDetailRMV(cluster_id, ver_num, node_id, node_list)

  def asClusterDetailADD(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]):
  ClusterDetailADD[NODE_ID, CLUSTER_ID] = cluster_detail.asInstanceOf[ClusterDetailADD[NODE_ID, CLUSTER_ID]]
		
  def asClusterDetailRMV(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]):
  ClusterDetailRMV[NODE_ID, CLUSTER_ID] = cluster_detail.asInstanceOf[ClusterDetailRMV[NODE_ID, CLUSTER_ID]]

  def get_node_id(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]):
  NODE_ID = cluster_detail match {
    case _: TYPE_CLUSTER_DETAIL_ADD => asClusterDetailADD(cluster_detail).node_id
    case _: TYPE_CLUSTER_DETAIL_RMV => asClusterDetailRMV(cluster_detail).node_id
  }
	
  def get_node_list(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]):
  List[NODE_ID] = cluster_detail match {
    case _: TYPE_CLUSTER_DETAIL_ADD => asClusterDetailADD(cluster_detail).node_list
    case _: TYPE_CLUSTER_DETAIL_RMV => asClusterDetailRMV(cluster_detail).node_list
  }
	
  def get_cluster_id(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]):
  CLUSTER_ID = cluster_detail match {
    case _: TYPE_CLUSTER_DETAIL_ADD => asClusterDetailADD(cluster_detail).cluster_id
    case _: TYPE_CLUSTER_DETAIL_RMV => asClusterDetailRMV(cluster_detail).cluster_id
  }
	
  def get_ver_num(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]):
  CLUSTER_VER_NUM = cluster_detail match {
    case _: TYPE_CLUSTER_DETAIL_ADD => asClusterDetailADD(cluster_detail).ver_num
    case _: TYPE_CLUSTER_DETAIL_RMV => asClusterDetailRMV(cluster_detail).ver_num
  }

  def get_vclock(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID])
	        (implicit vectorClock: VectorClock[NODE_ID]):
  VCLOCK[NODE_ID] = vectorClock.create(get_node_list(cluster_detail))
		
  def valid_node_list(cluster_detail0: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
                      cluster_detail1: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID]):
  Boolean = get_node_list(cluster_detail1).diff(get_node_list(cluster_detail0)).size == 0
	
  def comp_vn(v0: CLUSTER_VER_NUM, v1: CLUSTER_VER_NUM):
  COMPCC = v0.compareTo(v1) match {
    case 1  => GTCC
    case 0  => EQCC
    case -1 => LTCC 
  }
	
  def comp_cc(mcluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
              scluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	      anyId: AnyId[CLUSTER_ID]):
  COMPCC = anyId.eqv(get_cluster_id(mcluster_detail), get_cluster_id(scluster_detail)) match {
    case true  => comp_vn(get_ver_num(mcluster_detail), get_ver_num(scluster_detail))
    case false => IVCC
  }
}
