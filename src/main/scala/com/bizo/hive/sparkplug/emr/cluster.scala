package com.bizo.hive.sparkplug.emr

import scala.language.implicitConversions

import com.amazonaws.services.elasticmapreduce.model.SpotProvisioningTimeoutAction


abstract sealed class MasterDef
abstract sealed class TaskDef
abstract sealed class CoreDef
abstract sealed class UnspecifiedDef

class Cluster[M, C, T](val instances: Set[InstanceGroup]) {
  def +(m: Master)(implicit ev: M =:= UnspecifiedDef) = {
    new Cluster[MasterDef, C, T](instances + m)
  }
  
  def +(t: Task)(implicit ev: T =:= UnspecifiedDef) = {
    new Cluster[M, C, TaskDef](instances + t)
  }
  
  def +(c: Core)(implicit ev: C =:= UnspecifiedDef) = {
    new Cluster[M, CoreDef, T](instances + c)
  }
}

abstract sealed class MasterFleetDef
abstract sealed class TaskFleetDef
abstract sealed class CoreFleetDef
abstract sealed class UnspecifiedFleetDef

class InstanceFleetCluster[M, C, T](val fleets: Set[InstanceFleet]) {
  def +(m: MasterFleet)(implicit ev: M =:= UnspecifiedFleetDef) = {
    new InstanceFleetCluster[MasterFleetDef, C, T](fleets + m)
  }

  def +(t: TaskFleet)(implicit ev: T =:= UnspecifiedFleetDef) = {
    new InstanceFleetCluster[M, C, TaskFleetDef](fleets + t)
  }

  def +(c: CoreFleet)(implicit ev: C =:= UnspecifiedDef) = {
    new InstanceFleetCluster[M, CoreFleetDef, T](fleets + c)
  }
}

sealed trait RunnableCluster

class RunnableFleets(val fleets: Set[InstanceFleet]) extends RunnableCluster

trait ClusterImplicits {
  implicit def masterToRunnableFleet(m: MasterFleet): RunnableCluster = new RunnableFleets(Set(m))
  implicit def masterOnlyToRunnableFleet(c: InstanceFleetCluster[MasterFleetDef, UnspecifiedFleetDef, UnspecifiedFleetDef]): RunnableCluster = new RunnableFleets(c.fleets)

  // Task is only allowed if Core is present.
  implicit def masterCoreToRunnableFleet(c: InstanceFleetCluster[MasterFleetDef, CoreFleetDef, _]): RunnableCluster = new RunnableFleets(c.fleets)

  implicit def masterToRunnable(m: Master): RunnableCluster = new RunnableGroups(Set(m))
  implicit def masterOnlyToRunnable(c: Cluster[MasterDef, UnspecifiedDef, UnspecifiedDef]): RunnableCluster = new RunnableGroups(c.instances)

  // Task is only allowed if Core is present.
  implicit def masterCoreToRunnable(c: Cluster[MasterDef, CoreDef, _]): RunnableCluster = new RunnableGroups(c.instances)

  implicit def strToFleetInstance(instance: String): FleetInstance = FleetInstance(instance)
}

class RunnableGroups(val instances: Set[InstanceGroup]) extends RunnableCluster

trait BidProvider {
  def bid(size: String): Double
}

class FixedBidProvider(bid: Double) extends BidProvider {
  def bid(size: String): Double = bid
}

/**
  * Configures an instance fleet.
  *
  * Based on the instance weight, EMR will provision as many instances as are required to satisfy your
  * capacity requirement, possibly exceeding it. For example, if you only specified a single
  * FleetInstance, r4.8xlarge, with a weight of 32, and requested 64 on-demand capacity, you would
  * end up receiving 2 r4.8xlarges. If you specified r4.xlarge through r4.8xlarge FleetInstances
  * and requested 64 spot capacity, along with the default instance weights, you may end up with
  * two r4.8xlarges, or 4 r4.4xlarges, or 8 r4.2xlarges, or 16 r4.xlarges, or 1 r4.8xlarge and
  * 2 r4.4xlarges, among other options, all based on available spot capacity and current instance
  * pricing.
  *
  * @param instances Each instance fleet allows up to 5 instance types to be selected by EMR
  *                  based on the requested capacity and weighting.
  * @param name Required name of the fleet
  * @param onDemandCapacity Arbitrary capacity units to be satisfied by on-demand instances.
  * @param spotCapacity Arbitrary capacity units to be satisfied by spot instances.
  * @param spotSpecification Additional spot instance configuration options
  * @param instanceWeighting A function that specifies the weight for each instance type.
  *
  */
abstract class InstanceFleet(val instances: Seq[FleetInstance], val name: String, val onDemandCapacity: Int, val spotCapacity: Int,
  val spotSpecification: Option[SpotSpecification], val instanceWeighting: (String) => Int) {
  def fleetType: String
}

/**
  * Describes additional configuration specific to spot instances if an InstanceFleet has allocated
  * any spot capacity.
  *
  * Defined duration instance fleets use different pricing from the spot market. See
  * https://aws.amazon.com/ec2/spot/pricing/ for more information.
  *
  * @param spotDurationMinutes Allows you to request up to 360 minutes of running time, during which
  *                            your spot instances are guaranteed to not be interrupted.
  * @param timeoutAction Specifies what to do if spot capacity cannot be allocated (due to
  *                      capacity or an insufficiently low bid price). Defaults to switching to
  *                      on-demand instances.
  * @param timeoutMinutes Specifies how long to wait for spot capacity to be fulfilled before
  *                       taking another action, specified by the timeoutAction parameter.
  */
case class SpotSpecification(spotDurationMinutes: Int,
  timeoutAction: SpotProvisioningTimeoutAction = SpotProvisioningTimeoutAction.SWITCH_TO_ON_DEMAND,
  timeoutMinutes: Int = 10)

class MasterFleet(instances: Seq[FleetInstance], name: String, onDemandCapacity: Int, spotCapacity: Int,
  spotSpecification: Option[SpotSpecification]) extends InstanceFleet(instances, name, onDemandCapacity, spotCapacity, spotSpecification, InstanceWeighting.flatWeighter) {
  override def fleetType: String = "MASTER"
}

object MasterFleet {
  def apply(instance: FleetInstance, onDemandCapacity: Int, spotCapacity: Int,
    spotSpecification: Option[SpotSpecification]): MasterFleet = new MasterFleet(Seq(instance), "Master",
    onDemandCapacity, spotCapacity, spotSpecification)
  def apply(instance: String): MasterFleet = new MasterFleet(Seq(FleetInstance(instance)), "Master", 1, 0, None)
}

class CoreFleet(instances: Seq[FleetInstance], name: String, onDemandCapacity: Int, spotCapacity: Int,
  spotSpecification: Option[SpotSpecification], instanceWeighting: (String) => Int = InstanceWeighting.vCpuWeighter) extends InstanceFleet(instances, name,
  onDemandCapacity, spotCapacity, spotSpecification, instanceWeighting) {
  override def fleetType: String = "CORE"
}

object CoreFleet {
  def apply(instanceType: String, onDemandInstances: Int, spotInstances: Int, spotSpecification: Option[SpotSpecification]): CoreFleet = {
    new CoreFleet(Seq(FleetInstance(instanceType)), "Core", onDemandInstances, spotInstances, spotSpecification, InstanceWeighting.flatWeighter)
  }
  def apply(instanceTypes: Seq[String], onDemandCapacity: Int, spotCapacity: Int, spotSpecification: Option[SpotSpecification]): CoreFleet = {
    new CoreFleet(instanceTypes.map(FleetInstance(_)), "Core", onDemandCapacity, spotCapacity, spotSpecification)
  }
  def apply(instanceTypes: Seq[String], onDemandCapacity: Int, spotCapacity: Int, spotSpecification: Option[SpotSpecification],
    instanceWeighting: (String) => Int = InstanceWeighting.vCpuWeighter): CoreFleet = {
    new CoreFleet(instanceTypes.map(FleetInstance(_)), "Core", onDemandCapacity, spotCapacity, spotSpecification, instanceWeighting)
  }
}

class TaskFleet(instances: Seq[FleetInstance], name: String, onDemandCapacity: Int, spotCapacity: Int,
  spotSpecification: Option[SpotSpecification], instanceWeighting: (String) => Int = InstanceWeighting.vCpuWeighter) extends InstanceFleet(instances, name,
  onDemandCapacity, spotCapacity, spotSpecification, instanceWeighting) {
  override def fleetType: String = "TASK"
}

object TaskFleet {
  def apply(instanceType: String, onDemandInstances: Int, spotInstances: Int, spotSpecification: Option[SpotSpecification]): TaskFleet = {
    new TaskFleet(Seq(FleetInstance(instanceType)), "Task", onDemandInstances, spotInstances, spotSpecification, InstanceWeighting.flatWeighter)
  }
  def apply(instanceTypes: Seq[String], onDemandCapacity: Int, spotCapacity: Int, spotSpecification: Option[SpotSpecification]): TaskFleet = {
    new TaskFleet(instanceTypes.map(FleetInstance(_)), "Task", onDemandCapacity, spotCapacity, spotSpecification)
  }
  def apply(instanceTypes: Seq[String], onDemandCapacity: Int, spotCapacity: Int, spotSpecification: Option[SpotSpecification],
    instanceWeighting: (String) => Int = InstanceWeighting.vCpuWeighter): TaskFleet = {
    new TaskFleet(instanceTypes.map(FleetInstance(_)), "Task", onDemandCapacity, spotCapacity, spotSpecification, instanceWeighting)
  }
}

object FleetInstance {
  val DefaultSpotPricingStrategy: SpotPricingStrategy = SpotPricingStrategy()

  def apply(instanceType: String, ebsVolumes: Seq[EbsVolume],
    spotPricingStrategy: Option[SpotPricingStrategy] = Some(FleetInstance.DefaultSpotPricingStrategy)): FleetInstance = {
    new FleetInstance(instanceType, ebsVolumes, spotPricingStrategy)
  }

  def apply(instanceType: String): FleetInstance = {
    new FleetInstance(instanceType)
  }
}

class FleetInstance(val instanceType: String, val ebsVolumes: Seq[EbsVolume],
  val spotPricingStrategy: Option[SpotPricingStrategy] = Some(FleetInstance.DefaultSpotPricingStrategy)) {
  def this(instanceType: String) = {
    this(instanceType, Seq(EbsVolumeDefaults.getDefault(instanceType)).flatten)
  }
}

/**
  * Describes the spot pricing strategy to use for bidding on instances. Only one of
  * bidPrice or bidVsOnDemandPercent should be set.
  *
  * @param bidPrice Allows you to set a fixed bid amount.
  * @param bidVsOnDemandPercent Allows a variable bid relative to the percentage of the instance
  *                             type's on-demand price. Defaults to bidding up to 100% of the
  *                             on-demand price.
  */
case class SpotPricingStrategy(bidPrice: Option[String] = None, bidVsOnDemandPercent: Option[Int] = Some(100))

/**
  * Describes a set of Elastic Block Storage volumes to be attached to the instance. For many new
  * third and fourth-generation instance types, there is no local storage, and you must specify
  * EBS volumes.
  *
  * When no configuration is specified, a 32GB volume is attached to instances with no local
  * storage. This is typically insufficient for many workloads so [[EbsVolumeDefaults.getDefault]]
  * is used in the default apply methods for [[FleetInstance]] to specify relatively sane defaults.
  *
  * @param gbSize Size in gigabytes of the volume(s)
  * @param volType Volume type. Defaults to gp2, which are general-purpose SSD volumes with
  *                IOPS relative to the volume size. Other types at the time of writing are
  *                standard (don't use!), sc1 (Cold HDD), st1 (Throughput Optimized), and
  *                io1 (Provisioned IOPS)
  * @param volumes Number of volumes to attach. Defaults to 1.
  * @param iops Valid only for disks allowing provisioned IOPS (io1)
  */
case class EbsVolume(gbSize: Int, volType: String = "gp2", volumes: Int = 1, iops: Option[Int] = None)

object InstanceWeighting {
  /**
    * Instance weights in the EMR UI are specified as vCPUs. You might want to use other weights.
    */
  val defaultVCpuWeights: Map[String, Int] = Map(
    "c1.medium" -> 2,
    "c1.xlarge" -> 8,
    "c3.xlarge" -> 4,
    "c3.2xlarge" -> 8,
    "c3.4xlarge" -> 16,
    "c3.8xlarge" -> 32,
    "c4.large" -> 2,
    "c4.xlarge" -> 4,
    "c4.2xlarge" -> 8,
    "c4.4xlarge" -> 16,
    "c4.8xlarge" -> 36,
    "cc2.8xlarge" -> 32,
    "cg1.4xlarge" -> 16,
    "cr1.8xlarge" -> 32,
    "d2.xlarge" -> 8,
    "d2.2xlarge" -> 16,
    "d2.4xlarge" -> 32,
    "d2.8xlarge" -> 80,
    "g2.2xlarge" -> 8,
    "hi1.4xlarge" -> 16,
    "hs1.8xlarge" -> 64,
    "i2.xlarge" -> 8,
    "i2.2xlarge" -> 16,
    "i2.4xlarge" -> 32,
    "i2.8xlarge" -> 64,
    "m1.medium" -> 1,
    "m1.large" -> 2,
    "m1.xlarge" -> 4,
    "m2.xlarge" -> 2,
    "m2.2xlarge" -> 4,
    "m2.4xlarge" -> 8,
    "m3.xlarge" -> 8,
    "m3.2xlarge" -> 16,
    "m4.large" -> 4,
    "m4.xlarge" -> 8,
    "m4.2xlarge" -> 16,
    "m4.4xlarge" -> 32,
    "m4.10xlarge" -> 80,
    "m4.16xlarge" -> 128,
    "r3.xlarge" -> 8,
    "r3.2xlarge" -> 16,
    "r3.4xlarge" -> 32,
    "r3.8xlarge" -> 64,
    "r4.xlarge" -> 4,
    "r4.2xlarge" -> 8,
    "r4.4xlarge" -> 16,
    "r4.8xlarge" -> 32,
    "r4.16xlarge" -> 64
  )

  def mapWeighter(weights: Map[String, Int]): (String) => Int = (instance: String) => weights(instance)

  val vCpuWeighter: (String) => Int = mapWeighter(defaultVCpuWeights)

  /**
    * If you only have one instance type in your fleet, then you can assign a weight of 1 and
    * the capacity specification becomes a simple count of desired instances.
    */
  val flatWeighter: (String) => Int = (_) => 1
}

object EbsVolumeDefaults {
  private val instanceDiskSize = Map(
    "m4.large" -> 40,
    "m4.xlarge" -> 80,
    "m4.2xlarge" -> 160,
    "m4.4xlarge" -> 320,
    "m4.10xlarge" -> 800,
    "m4.16xlarge" -> 1280,
    "c4.large" -> 40,
    "c4.xlarge" -> 80,
    "c4.2xlarge" -> 160,
    "c4.4xlarge" -> 320,
    "c4.8xlarge" -> 640,
    "p2.xlarge" -> 80,
    "p2.8xlarge" -> 640,
    "p2.16xlarge" -> 1280,
    "r4.large" -> 40,
    "r4.xlarge" -> 80,
    "r4.2xlarge" -> 160,
    "r4.4xlarge" -> 320,
    "r4.8xlarge" -> 640,
    "r4.16xlarge" -> 1280
  )

  /**
    * For instance types that have no attached storage, this method will try to return a sensible
    * default attached storage size similar to prior-generation instances that came with
    * local SSDs.
    *
    * @param instanceType The type of instance that needs attached storage.
    * @return Optionally returns an EbsVolume. If no default is available, then None is returned.
    */
  def getDefault(instanceType: String): Option[EbsVolume] = {
    instanceDiskSize.get(instanceType).map { size =>
      new EbsVolume(size)
    }
  }
}

abstract class InstanceGroup(val num: Int, val size: Option[String], val bid: Option[BidProvider] = None)

class Master(size: Option[String]) extends InstanceGroup(1, size)

object Master {
  def apply(size: String) = new Master(Some(size))
  def apply() = new Master(None)
}

class Core(num: Int, size: Option[String]) extends InstanceGroup(num, size)

object Core {
  def apply(num: Int, size: String) = new Core(num, Some(size))
  def apply(num: Int) = new Core(num, None)
}

class Task(num: Int, size: Option[String], bid: Option[BidProvider]) extends InstanceGroup(num, size, bid)

object Task {
  def apply(num: Int, size: String) = new Task(num, Some(size), None)
  def apply(num: Int) = new Task(num, None, None)
  def apply(num: Int, size: String, bid: Double) = new Task(num, Some(size), Some(new FixedBidProvider(bid)))
  def apply(num: Int, bid: Double) = new Task(num, None, Some(new FixedBidProvider(bid)))
}

object Spot {
  def apply(num: Int)(implicit bidProvider: BidProvider) = new Task(num, None, Some(bidProvider))
  def apply(num: Int, size: String)(implicit bidProvider: BidProvider) = new Task(num, Some(size), Some(bidProvider))
  def apply(num: Int, size: String, bid: Double) = new Task(num, Some(size), Some(new FixedBidProvider(bid)))
}
