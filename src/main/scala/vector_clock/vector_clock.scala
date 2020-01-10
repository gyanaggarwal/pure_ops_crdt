package vector_clock

import model.Model._

sealed trait COMPLC
final case object LTLC   extends COMPLC
final case object EQLC   extends COMPLC
final case object GTLC   extends COMPLC

sealed trait CHECKMSG
final case object DUPMSG extends CHECKMSG
final case object NXTMSG extends CHECKMSG
final case object OOOMSG extends CHECKMSG

sealed trait COMPVC
final case object UNDVC  extends COMPVC
final case object LTVC   extends COMPVC
final case object EQVC   extends COMPVC
final case object GTVC   extends COMPVC
final case object CONVC  extends COMPVC

trait VectorClock[NODE_ID]{
  val zero: LOGICAL_CLOCK = 0
	
  def empty: VCLOCK[NODE_ID]
	
  def merge_vc(vc0: VCLOCK[NODE_ID],
               vc1: VCLOCK[NODE_ID],
               mf: (LOGICAL_CLOCK, LOGICAL_CLOCK) => LOGICAL_CLOCK): 
  VCLOCK[NODE_ID]
	
  def create(node_list: List[NODE_ID]): 
  VCLOCK[NODE_ID] = node_list.foldLeft(empty)((vc0, n0) => vc0.updated(n0, zero))
		
  def next_clock(lc: LOGICAL_CLOCK): 
  LOGICAL_CLOCK = lc+1 

  def comp_lc(lc0: LOGICAL_CLOCK, lc1: LOGICAL_CLOCK): 
  COMPLC = lc0.compareTo(lc1) match {
    case -1 => LTLC
    case 0  => EQLC
    case 1  => GTLC
  } 
	
  def max(lc0: LOGICAL_CLOCK, lc1: LOGICAL_CLOCK): 
  LOGICAL_CLOCK = scala.math.max(lc0, lc1)
	
  def min(lc0: LOGICAL_CLOCK, lc1: LOGICAL_CLOCK): 
  LOGICAL_CLOCK = scala.math.min(lc0, lc1)
	
  def remove(node_id: NODE_ID, vc: VCLOCK[NODE_ID]):
  VCLOCK[NODE_ID] = vc - node_id
		
  def logical_clock(node_id: NODE_ID, vc: VCLOCK[NODE_ID]): 
  LOGICAL_CLOCK = vc.getOrElse(node_id, zero)
	
  def next(node_id: NODE_ID, vc: VCLOCK[NODE_ID]): 
  VCLOCK[NODE_ID] = {
    val lc: LOGICAL_CLOCK = logical_clock(node_id, vc)
    vc.updated(node_id, next_clock(lc))
  }
	
  def merge_max(vc0: VCLOCK[NODE_ID], vc1: VCLOCK[NODE_ID]): 
  VCLOCK[NODE_ID] = merge_vc(vc0, vc1, max)
	
  def merge_min(vc0: VCLOCK[NODE_ID], vc1: VCLOCK[NODE_ID]): 
  VCLOCK[NODE_ID] = merge_vc(vc0, vc1, min)
	
  def merge_undelivered(node_id: NODE_ID, 
		        pvc: VCLOCK[NODE_ID], 
			mvc: VCLOCK[NODE_ID]): 
  VCLOCK[NODE_ID] = merge_max(pvc, remove(node_id, mvc))

  def merge_logical_clock(mnode_id: NODE_ID,
                          mvc: VCLOCK[NODE_ID],
		          nvc: VCLOCK[NODE_ID]): 
  VCLOCK[NODE_ID] = {
    val mlc = logical_clock(mnode_id, mvc)
    val nlc = logical_clock(mnode_id, nvc)
    nvc.updated(mnode_id, max(mlc, nlc))					 	
  }

  def check_msg(mnode_id: NODE_ID,
                mvc: VCLOCK[NODE_ID],
		nvc: VCLOCK[NODE_ID]): 
  CHECKMSG = {
    val mlc = logical_clock(mnode_id, mvc)
    val nlc = next_clock(logical_clock(mnode_id, nvc))
    comp_lc(mlc, nlc) match {
      case LTLC => DUPMSG
      case EQLC => NXTMSG
      case GTLC => OOOMSG
    }
  }

  def comp_vclc(compvc: COMPVC, 
		lc0: LOGICAL_CLOCK, 
		lc1: LOGICAL_CLOCK): 
  COMPVC = (compvc, comp_lc(lc0, lc1)) match {
    case (UNDVC, LTLC) => LTVC
    case (UNDVC, EQLC) => EQVC
    case (UNDVC, GTLC) => GTVC
    case (LTVC, GTLC)  => CONVC
    case (LTVC, _)     => LTVC
    case (EQVC, LTLC)  => LTVC
    case (EQVC, EQLC)  => EQVC
    case (EQVC, GTLC)  => GTVC
    case (GTVC, LTLC)  => CONVC
    case (GTVC, _)     => GTVC
    case (CONVC, _)    => CONVC 
  }
	
  def comp_vc(vc0: VCLOCK[NODE_ID],
              vc1: VCLOCK[NODE_ID]): 
  COMPVC = { 
    vc0.foldLeft(UNDVC: COMPVC){case (compvc0, (ni0, lc0)) => 
      vc1.get(ni0).fold(compvc0)(lc1 => comp_vclc(compvc0, lc0, lc1))
    }         	
  }
	
  def equal(vc0: VCLOCK[NODE_ID],
            vc1: VCLOCK[NODE_ID]): 
  Boolean = comp_vc(vc0, vc1) match {
    case EQVC => true
    case _    => false
  }
}
