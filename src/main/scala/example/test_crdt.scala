package example

import model.Model._
import pure_ops._
import pure_ops.implementation._
import middleware._
import cluster_config.interpreter._
import vector_clock._
import vector_clock.interpreter._
import message._
import message.interpreter._
import msg_data.interpreter._
import po_log.interpreter._
import util.interpreter._
import util._
 
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
	implicit val conMsgList = CONMsgListInstances.uCONMsgList
	
	val ianyId = AnyIdInstances.intAnyId
  val node_id0: UNODE_ID = 0
	val node_id1: UNODE_ID = 1
	val node_id2: UNODE_ID = 2
	val node_list3: List[UNODE_ID] = List(node_id0, node_id1, node_id2)
	val node_list2: List[UNODE_ID] = List(node_id0, node_id1)
	val node_list1: List[UNODE_ID] = List(node_id0)
	val cluster_id: UCLUSTER_ID = 0
	val ver_num0: CLUSTER_VER_NUM = 0
	val ver_num1: CLUSTER_VER_NUM = 1
	val crdt_id0: UCRDT_ID = 0
	val crdt_id1: UCRDT_ID = 1
	
	val cda001 = clusterConfig.makeADD(cluster_id, ver_num0, node_id0, node_list1)
	val cda003 = clusterConfig.makeADD(cluster_id, ver_num0, node_id0, node_list3)
	val cda013 = clusterConfig.makeADD(cluster_id, ver_num0, node_id1, node_list3)
	val cda023 = clusterConfig.makeADD(cluster_id, ver_num0, node_id2, node_list3)
	val cda002 = clusterConfig.makeADD(cluster_id, ver_num0, node_id0, node_list2)
	val cda012 = clusterConfig.makeADD(cluster_id, ver_num0, node_id1, node_list2)
	val cda123 = clusterConfig.makeADD(cluster_id, ver_num1, node_id2, node_list3)
	val cdr122 = clusterConfig.makeRMV(cluster_id, ver_num1, node_id2, node_list2)

	val pnc0 = crdtInstance.make(PNCounter, crdt_id0)
	val aws0 = crdtInstance.make(AWSet, crdt_id0)
	val pnc1 = crdtInstance.make(PNCounter, crdt_id1)
	val aws1 = crdtInstance.make(AWSet, crdt_id1)
	
	val csd0001 = crdtState.empty(node_id0, cda001)

	val csd0003 = crdtState.empty(node_id0, cda003)
	val csd1013 = crdtState.empty(node_id1, cda013)
	val csd2023 = crdtState.empty(node_id2, cda023)

	val csd0002 = crdtState.empty(node_id0, cda002)
	val csd1012 = crdtState.empty(node_id1, cda012)
	val csd2123 = crdtState.make(node_id2, cda123)

	val ump0 = userMSG.make(pnc0, INC())
	val uma5 = userMSG.make(aws0, ADD(5))
	val uma6 = userMSG.make(aws0, ADD(6))
	val uma7 = userMSG.make(aws0, ADD(7))
	
	val ulist0 = List(ump0, uma5)
	val ulist1 = List(ump0, uma6)
	val ulist2 = List(ump0, uma7)
	
	def add_con_msg() = {
		val u01 = msgOpr.make_msg_ops(nodeVCLOCK.make(node_id0, 
			                                            vectorClock.make(List((node_id0, 1),
																								                        (node_id1, 0),
																																			  (node_id2, 0)))), 
																	aws0, ADD(5))
		val u02 = msgOpr.make_msg_ops(nodeVCLOCK.make(node_id0, 
																		              vectorClock.make(List((node_id0, 2),
																																				(node_id1, 0),
																																				(node_id2, 0)))), 
																	aws0, ADD(5))
																	
		val u11 = msgOpr.make_msg_ops(nodeVCLOCK.make(node_id1, 
			                                            vectorClock.make(List((node_id0, 0),
																																				(node_id1, 1),
																																				(node_id2, 0)))), 
																	aws0, ADD(5))
		val u12 = msgOpr.make_msg_ops(nodeVCLOCK.make(node_id1, 
																									vectorClock.make(List((node_id0, 1),
																																				(node_id1, 2),
																																				(node_id2, 0)))), 
																	aws0, ADD(5))
																	
		val u21 = msgOpr.make_msg_ops(nodeVCLOCK.make(node_id2, 
																		              vectorClock.make(List((node_id0, 2),
																																				(node_id1, 1),
																																				(node_id2, 1)))), 
																	aws0, ADD(5))
		val u22 = msgOpr.make_msg_ops(nodeVCLOCK.make(node_id2, 
																									vectorClock.make(List((node_id0, 2),
																																				(node_id1, 2),
																																				(node_id2, 2)))), 
																	aws0, ADD(5))
																	
		val l0 = List(u01, u02, u11, u21, u12, u22)
		val l1 = List(u11, u01, u12, u02, u21, u22)
		val l2 = List(u01, u11, u02, u21, u12, u22)
		
		val cml = conMsgList.empty
		val cml0 = l0.foldLeft(cml)((cmlx0, msg0) => conMsgList.add_msg(msg0, cmlx0))
		val cml1 = l1.foldLeft(cml)((cmlx0, msg0) => conMsgList.add_msg(msg0, cmlx0))
		val cml2 = l2.foldLeft(cml)((cmlx0, msg0) => conMsgList.add_msg(msg0, cmlx0))
		
		(cml0, cml1, cml2)
	}

  def split_con_msg(vclock: VCLOCK[UNODE_ID],
	                  cml: CON_MSG_LIST[UNODE_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) =
		conMsgList.split_msg(vclock, cml)
		
	def update_from_user(ulist: List[USER_MSG[PureOpsCRDT, UCRDT_ID, CRDTOps]], 
		                   csdx: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = 
    UpdateFromUser.update_list(ulist, csdx, ianyId)										 

  def mupdate_from_user() = (update_from_user(ulist0, csd0003), 
	                           update_from_user(ulist1, csd1013), 
														 update_from_user(ulist2, csd2023))

	def update_from_peer_smd(smdx: SND_MSG_DATA[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											     csdx: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = 
	  smdx.get(crdtState.get_node_id(csdx)).fold(csdx)(umcx => update_from_peer(umcx, csdx))
														  
  def update_from_peer(umcx: UNDELIV_MSG_CLASS[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                     csdx: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) =
	  UpdateFromPeer.update(umcx, csdx, ianyId, ianyId)
		
	def remove_from_list(tnode_list: List[UNODE_ID],
		                   csdx: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = 
		GenUtil.remove_from_list(crdtState.get_node_id(csdx), tnode_list, ianyId)
		
	def mupdate_from_peer3(csdx0: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                       csdx1: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
											   csdx2: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = {
	  val smd0 = snd_msg_data(node_list3, csdx0)
		val smd1 = snd_msg_data(node_list3, csdx1)
		val smd2 = snd_msg_data(node_list3, csdx2)
		
		val csdx01 = update_from_peer_smd(smd1, csdx0)	
		val csdx02 = update_from_peer_smd(smd2, csdx01)										  	

		val csdx11 = update_from_peer_smd(smd0, csdx1)	
		val csdx12 = update_from_peer_smd(smd2, csdx11)										  	

		val csdx21 = update_from_peer_smd(smd0, csdx2)	
		val csdx22 = update_from_peer_smd(smd1, csdx21)										  	
		 									  	
		(csdx02, csdx12, csdx22)											
	}
		
	def mupdate_from_peer2(tnode_list: List[UNODE_ID],
		                     csdx0: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps],
	                       csdx1: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) = {
	  val smd0 = snd_msg_data(tnode_list, csdx0)
		val smd1 = snd_msg_data(tnode_list, csdx1)
		
		val csdx01 = update_from_peer_smd(smd1, csdx0)	

		val csdx11 = update_from_peer_smd(smd0, csdx1)	

		(csdx01, csdx11)											
	}
	
	def add_replica() = {
	  val csdx00 = update_from_user(ulist0, csd0002)	
		val csdx10 = update_from_user(ulist1, csd1012)
		val csdx20 = csd2123
		
		val csdx01 = crdtState.upgrade_replica(cda123, csdx00)
		val csdx11 = csdx10
		val csdx21 = crdtState.new_replica(csdx01, csdx20, ianyId)
		
		(csdx01, csdx11, csdx21)
	}
	
	def rmv_replica(tnode_list: List[UNODE_ID]) = {
		val csdx00 = update_from_user(List(uma5), csd0003)
		val csdx10 = update_from_user(List(uma6), csd1013)
		val csdx20 = update_from_user(List(uma7), csd2023)
		
		val smd20 = snd_msg_data(tnode_list, csdx20)
		
		val csdx01 = update_from_peer_smd(smd20, csdx00)
		(csdx01, csdx10)
	}
	
	def upgrade_replica(cdx: CLUSTER_DETAIL[UNODE_ID, UCLUSTER_ID],
	                    csdx: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) =
	crdtState.upgrade_replica(cdx, csdx)
	
	def query_from_user(crdt_instance: CRDT_INSTANCE[PureOpsCRDT, UCRDT_ID],
	                    crdt_ops: CRDTOps,
										  csdx: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) =
	  QueryFromUser.eval(crdt_instance, crdt_ops, csdx)
		
	def snd_msg_data(tnode_list: List[UNODE_ID],
		               csdx: CRDT_STATE[UNODE_ID, UCLUSTER_ID, PureOpsCRDT, UCRDT_ID, CRDTOps]) =
	  MSGSndRcv.snd_msg_data(tnode_list, csdx, ianyId)
}

