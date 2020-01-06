package vector_clock

import model.Model._
import cluster_config._
import util._

trait TCSB[NODE_ID, CLUSTER_ID] {
	def empty: PEER_VCLOCK[NODE_ID]
	
	def get_node_vclock(tcsb_class: TCSB_CLASS[NODE_ID]):
	VCLOCK[NODE_ID] = tcsb_class.node_vclock

	def get_peer_vclock(tcsb_class:TCSB_CLASS[NODE_ID]):
	PEER_VCLOCK[NODE_ID] = tcsb_class.peer_vclock
	
	def set_node_vclock(nvc: VCLOCK[NODE_ID],
	                    tcsb_class: TCSB_CLASS[NODE_ID]):
	TCSB_CLASS[NODE_ID] = tcsb_class.copy(node_vclock = nvc)
	
	def set_peer_vclock(pvc: PEER_VCLOCK[NODE_ID],
		                  tcsb_class: TCSB_CLASS[NODE_ID]):
	TCSB_CLASS[NODE_ID] = tcsb_class.copy(peer_vclock = pvc)
	
  def create(node_id: NODE_ID,
             node_list: List[NODE_ID],
						 anyId: AnyId[NODE_ID])
			      (implicit vectorClock: VectorClock[NODE_ID]):
  TCSB_CLASS[NODE_ID] = {
	  val evc: VCLOCK[NODE_ID] = vectorClock.create(node_list)
    val pvc: PEER_VCLOCK[NODE_ID] =
    GenUtil.remove_from_list(node_id, node_list, anyId)
		         .foldLeft(empty)((pvc0, node_id0) => pvc0.updated(node_id0, evc))
		TCSB_CLASS(evc, pvc)				  
  }	

  def merge_peer(mnode_id: NODE_ID,
                 mvc: VCLOCK[NODE_ID],
		             tcsb_class: TCSB_CLASS[NODE_ID])
		            (implicit vectorClock: VectorClock[NODE_ID]):
  TCSB_CLASS[NODE_ID] = {
		val peer_vclock0 = get_peer_vclock(tcsb_class)
    peer_vclock0.get(mnode_id)
		            .fold(tcsb_class)(pvc0 => {
		  val pvc1 = vectorClock.merge_max(pvc0, mvc)
			set_peer_vclock(peer_vclock0.updated(mnode_id, pvc1), tcsb_class)                           	
		})
  }

  def merge_peer_undelivered(mnode_id: NODE_ID,
                             mvc: VCLOCK[NODE_ID],
	                           tcsb_class: TCSB_CLASS[NODE_ID])
	                          (implicit vectorClock: VectorClock[NODE_ID]):
  TCSB_CLASS[NODE_ID] = {
	  val peer_vclock0 = get_peer_vclock(tcsb_class)
    peer_vclock0.get(mnode_id)
	              .fold(tcsb_class)(pvc0 => {
	    val pvc1 = vectorClock.merge_undelivered(mnode_id, pvc0, mvc)
		  set_peer_vclock(peer_vclock0.updated(mnode_id, pvc1), tcsb_class)                           	
	  })
  }

  def merge(mnode_id: NODE_ID,
	          mvc: VCLOCK[NODE_ID],
					  tcsb_class: TCSB_CLASS[NODE_ID])
					 (implicit vectorClock: VectorClock[NODE_ID]):
	TCSB_CLASS[NODE_ID] = {
		val nvc = vectorClock.merge_logical_clock(mnode_id, mvc, get_node_vclock(tcsb_class))
		merge_peer(mnode_id, mvc, set_node_vclock(nvc, tcsb_class))
	} 

	def next(rnode_id: NODE_ID,
	         tcsb_class: TCSB_CLASS[NODE_ID])
					(implicit vectorClock: VectorClock[NODE_ID]):
	(TCSB_CLASS[NODE_ID], VCLOCK[NODE_ID]) = {
	  val	nvc = vectorClock.next(rnode_id, get_node_vclock(tcsb_class))
		(set_node_vclock(nvc, tcsb_class), nvc)
	}
	
  def causal_stable(tcsb_class: TCSB_CLASS[NODE_ID])
                   (implicit vectorClock: VectorClock[NODE_ID]):
  VCLOCK[NODE_ID] = 
    get_peer_vclock(tcsb_class)
		  .foldLeft(get_node_vclock(tcsb_class))((ivc, tvc) => vectorClock.merge_min(ivc, tvc._2))

  def check_msg(mnode_id: NODE_ID,
                mvc: VCLOCK[NODE_ID],
		            tcsb_class: TCSB_CLASS[NODE_ID])
		           (implicit vectorClock: VectorClock[NODE_ID]): 
  CHECKMSG = vectorClock.check_msg(mnode_id, mvc, get_node_vclock(tcsb_class))

  def check_consistency(node_id: NODE_ID,
		                    tcsb_class: TCSB_CLASS[NODE_ID])
	                     (implicit vectorClock: VectorClock[NODE_ID]):
	Boolean = {
		val nvc = get_node_vclock(tcsb_class)
		val pvc = get_peer_vclock(tcsb_class)
		val nlc = vectorClock.logical_clock(node_id, nvc)
		pvc.foldLeft(true){case (flag, (pni0, pvc0)) => {
			flag match {
				case true  => vectorClock.logical_clock(pni0, nvc) == vectorClock.logical_clock(pni0, pvc0) &&
				              nlc >= vectorClock.logical_clock(node_id, pvc0)
				case false => false
			}
		}}
	}  

  def upgrade_replica(cluster_detail: CLUSTER_DETAIL[NODE_ID, CLUSTER_ID],
	                    tcsb_class: TCSB_CLASS[NODE_ID])
										 (implicit vectorClock: VectorClock[NODE_ID],
										           clusterConfig: ClusterConfig[NODE_ID, CLUSTER_ID]):
	TCSB_CLASS[NODE_ID] = { 
	  val node_id = clusterConfig.get_node_id(cluster_detail)
		cluster_detail match {
			case _: TYPE_CLUSTER_DETAIL_ADD => add_replica(node_id, 
				                                             clusterConfig.get_vclock(cluster_detail), 
																										 tcsb_class)
			case _: TYPE_CLUSTER_DETAIL_RMV => rmv_replica(node_id, tcsb_class)
		}	
	}
	
  def add_replica(add_node_id: NODE_ID,
                  defvc: VCLOCK[NODE_ID],
                  tcsb_class: TCSB_CLASS[NODE_ID])
		             (implicit vectorClock: VectorClock[NODE_ID]):
  TCSB_CLASS[NODE_ID] = {
    val nvc1 = vectorClock.merge_max(defvc, get_node_vclock(tcsb_class))
	  val pvc0 = get_peer_vclock(tcsb_class)
		val pvc1 = pvc0.mapValues(node_vc0 => vectorClock.merge_max(defvc, node_vc0))
    set_node_vclock(nvc1, set_peer_vclock(pvc1.updated(add_node_id, defvc), tcsb_class))		
  }

  def rmv_replica(rmv_node_id: NODE_ID,
                  tcsb_class: TCSB_CLASS[NODE_ID]):
	TCSB_CLASS[NODE_ID] = set_peer_vclock((get_peer_vclock(tcsb_class) - rmv_node_id), tcsb_class)		

  def new_replica(rnode_id: NODE_ID,
                  fnode_id: NODE_ID,
							    tcsb_class: TCSB_CLASS[NODE_ID]):
  TCSB_CLASS[NODE_ID] = {
	  val nvc = get_node_vclock(tcsb_class)
		val pvc = (get_peer_vclock(tcsb_class) - rnode_id).updated(fnode_id, nvc)
	  set_peer_vclock(pvc, tcsb_class)
  }				 
}