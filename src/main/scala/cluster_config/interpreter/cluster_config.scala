package cluster_config
package interpreter

import model.Model._

object ClusterConfigInstances {
  implicit val uClusterConfig: ClusterConfig[UNODE_ID, UCLUSTER_ID] = 
    new ClusterConfig[UNODE_ID, UCLUSTER_ID] {
    }
}
