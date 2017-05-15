package com.bizo.hive.sparkplug

import com.bizo.hive.sparkplug.s3.S3

package object emr extends ApplicationImplicits with ClusterImplicits {
  implicit def masterToCluster(m: Master) = new Cluster[MasterDef, UnspecifiedDef, UnspecifiedDef](Set(m))
  implicit def masterFleetToInstanceFleetCluster(m: MasterFleet) = new InstanceFleetCluster[MasterFleetDef, UnspecifiedDef, UnspecifiedDef](Set(m))
  implicit val defaultBidProvider: BidProvider = new DefaultBidProvider
  implicit lazy val s3: S3 = S3.defaultS3

}
