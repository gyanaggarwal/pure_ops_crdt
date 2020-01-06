package example

import model.Model._
import pure_ops._
import pure_ops.implementation._
import middleware._
import cluster_config.interpreter._
import vector_clock.interpreter._
import message.interpreter._
import msg_data.interpreter._
import po_log.interpreter._
import util.interpreter._
 
object TestCRDT {
	implicit val updateCRDT = UpdateCRDT
	implicit val msgSndRcv = MSGSndRcv
	implicit val crdtInstance = CRDTInstances.uCRDTInstance
	implicit val clusterConfig = ClusterConfigInstances.uClusterConfig
	implicit val userMSG = UserMSGInstances.uUserMSG
	implicit val vectorClock = VectorClockInstances.uVectorClock
	implicit val tcsb = TCSBInstances.uTCSB
	implicit val nodeVCLOCK = NodeVCLOCKInstances.uNodeVCLOCK
	implicit val msgOpr = MSGOperationInstances.uMSGOperation
	implicit val msgData = MSGDataInstances.uMSGData
	implicit val msgClass = MSGClassInstances.uMSGClass
	implicit val msgLog = MSGLogInstances.uMSGLog
	implicit val pologClass = POLogClassInstances.uPOLogClass
	implicit val polog = POLogInstances.uPOLog
	implicit val crdtState = CRDTStateInstances.uCRDTState
	
	val ianyId = AnyIdInstances.intAnyId
  val node_id0: UNODE_ID = 0
	val node_id1: UNODE_ID = 1
	val node_id2: UNODE_ID = 2
	val node_list: List[UNODE_ID] = List(node_id0, node_id1, node_id2)
	val cluster_id: UCLUSTER_ID = 0
	val ver_num: CLUSTER_VER_NUM = 0
	val crdt_id0: UCRDT_ID = 0
	val crdt_id1: UCRDT_ID = 1
	
	val cda0 = clusterConfig.makeADD(cluster_id, ver_num, node_id0, node_list)
	val cda1 = clusterConfig.makeADD(cluster_id, ver_num, node_id1, node_list)
	val cda2 = clusterConfig.makeADD(cluster_id, ver_num, node_id2, node_list)
	val pnc0 = crdtInstance.make(PNCounter, crdt_id0)
	val aws0 = crdtInstance.make(AWSet, crdt_id0)
	val pnc1 = crdtInstance.make(PNCounter, crdt_id1)
	val aws1 = crdtInstance.make(AWSet, crdt_id1)
	val csd0 = crdtState.empty(node_id0, cda0)
	val csd1 = crdtState.empty(node_id1, cda1)
	val csd2 = crdtState.empty(node_id2, cda2)
	
	val ump0 = userMSG.make(pnc0, INC())
	val uma5 = userMSG.make(aws0, ADD(5))
	val uma6 = userMSG.make(aws0, ADD(6))
	val uma7 = userMSG.make(aws0, ADD(7))
	
	val ulist0 = List(ump0, uma5)
	val ulist1 = List(ump0, uma6)
	val ulist2 = List(ump0, uma7)
	
	def update_from_user(ulist: List[USER_MSG[PureOpsCRDT, UCRDT_ID, CRDTOps]], 
		                   csd: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = 
	  ulist.foldLeft(csd)((csd0, um0) => UpdateFromUser.update(um0, csd0, ianyId))										 

  def mupdate_from_user() = (update_from_user(ulist0, csd0), 
	                           update_from_user(ulist1, csd1), 
														 update_from_user(ulist2, csd2))
														  
  def update_from_peer(umc: UNDELIV_MSG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                     csd: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) =
	  UpdateFromPeer.update(umc, csd, ianyId, ianyId)
		
	def mupdate_from_peer(csd0: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                      csd1: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											  csd2: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = {
	  val smd0 = snd_msg_data(csd0)
		val smd1 = snd_msg_data(csd1)
		val smd2 = snd_msg_data(csd2)
		
		val csd01 = update_from_peer(smd1(crdtState.get_node_id(csd0)), csd0)	
		val csd02 = update_from_peer(smd2(crdtState.get_node_id(csd0)), csd01)										  	

		val csd11 = update_from_peer(smd0(crdtState.get_node_id(csd1)), csd1)	
		val csd12 = update_from_peer(smd2(crdtState.get_node_id(csd1)), csd11)										  	

		val csd21 = update_from_peer(smd0(crdtState.get_node_id(csd2)), csd2)	
		val csd22 = update_from_peer(smd1(crdtState.get_node_id(csd2)), csd21)										  	
		 									  	
		(csd02, csd12, csd22)											
	}
		
	def query_from_user(crdt_instance: CRDT_INSTANCE[PureOpsCRDT, UCRDT_ID],
	                    crdt_ops: CRDTOps,
										  csd: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) =
	  QueryFromUser.eval(crdt_instance, crdt_ops, csd)
		
	def snd_msg_data(csd: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) =
	  MSGSndRcv.snd_msg_data(csd, ianyId)
}

