package pure_ops

import model.Model._
import vector_clock._
import message._
import msg_data._

import scala.collection.immutable._

sealed trait CRDTOps
final case class EVL(args: Any = ())   extends CRDTOps
final case class INC(args: Int = 1)    extends CRDTOps
final case class DEC(args: Int = 1)    extends CRDTOps
final case class ADD(args: Any)        extends CRDTOps
final case class RMV(args: Any)        extends CRDTOps 
final case class WRT(args: Any = ())   extends CRDTOps
final case class CLR(args: Any = ())   extends CRDTOps
final case class BCI(args: Any)        extends CRDTOps
final case class BCD(args: Any)        extends CRDTOps
final case class BCX(args: Any)        extends CRDTOps
final case class SET(args: Boolean)    extends CRDTOps

final case class ARSet(aset: HashSet[Any] = HashSet.empty[Any], 
	                     rset: HashSet[Any] = HashSet.empty[Any])

trait PureOpsCRDT {
	def init_data: Any

	def valid_ops(ops: CRDTOps): Boolean

  def isConcurrent(crdt_ops0: CRDTOps, crdt_ops1: CRDTOps): Boolean
	
  def update_comm_crdt(crdt_data: Any,
	                     crdt_ops: CRDTOps): Any

  def combine_msg_log_data(msg_log_data: Any,
	                         crdt_data: Any): Any
													 											 
	def process_msg_log(msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
										 (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
										 					 msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]): Any
	
	def combine_msg_log(crdt_data: Any,
		                  msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
										 (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
														 	 msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]): 
	Any = combine_msg_log_data(process_msg_log(msg_log), crdt_data)
	
	def update_causal_stable(crdt_data: Any,
		                       msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
										      (implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
														 	      msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]): 
	Any = combine_msg_log(crdt_data, msg_log)

  def eval_data(crdt_ops: CRDTOps,
	              crdt_data: Any):
	Any = crdt_data

  def eval_set(crdt_ops: CRDTOps,
	             crdt_set: HashSet[Any]):
	Any = crdt_ops match {
		case EVL(args) => args match {
			case () => crdt_set
			case _  => crdt_set.contains(args)
		}
		case _         => crdt_set
	}	
	
	def eval(crdt_ops: CRDTOps,
	         crdt_data: Any,
				   msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
					(implicit msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
										msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	Any = eval_data(crdt_ops, combine_msg_log(crdt_data, msg_log)) 
	
	def add_con_msg(msg_ops: MSG_OPS[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                con_msg_log: CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
								 (implicit vectorClock: VectorClock[UNODE_ID],
								           nodeVCLOCK: NodeVCLOCK[UNODE_ID],
												   conMSGLog: CONMSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   msgData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
												   msgLog: MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]

	def split_msg(csvc: VCLOCK[UNODE_ID],
	              con_msg_log: CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
							  msg_log: MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
							 (implicit vectorClock: VectorClock[UNODE_ID],
							           nodeVCLOCK: NodeVCLOCK[UNODE_ID],
											   conMSGLog: CONMSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   msgOpr: MSGOperation[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   msgData: MSGData[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   msgClass: MSGClass[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   msgLog: MSGLog[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]):
	(MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	 MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
   CON_MSG_LOG[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps])
}
