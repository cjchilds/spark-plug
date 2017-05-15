package com.bizo.hive.sparkplug.emr

import com.amazonaws.services.elasticmapreduce.model._
import scala.collection.JavaConverters._
import scala.language.postfixOps

import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}
import com.bizo.hive.sparkplug.auth._

class Emr private(credentials: Option[AWSCredentials]) {

  lazy val emr: AmazonElasticMapReduce = credentials match {
    case Some(knownCredentials) => AmazonElasticMapReduceClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(knownCredentials)).build()
    case None => AmazonElasticMapReduceClientBuilder.defaultClient()
  }

  def this() = this(None)

  def this(knownCredentials: AWSCredentials) = this(Some(knownCredentials))

  def run(flow: JobFlow, configureRequest: RunJobFlowRequest => RunJobFlowRequest = identity)
    (implicit config: ClusterConfig): String = {
    val request = new RunJobFlowRequest(flow.name, toJobFlowInstancesConfig(config, flow.cluster, flow.keepAlive, flow.terminationProtection))

    request.setAmiVersion(config.amiVersion.orNull)
    request.setReleaseLabel(config.releaseLabel.orNull)
    request.setLogUri(config.logUri.orNull)
    request.setVisibleToAllUsers(config.visibleToAllUsers.map(boolean2Boolean(_)).orNull)

    request.setJobFlowRole(config.jobFlowRole.orNull)
    request.setServiceRole(config.serviceRole.orNull)

    config.applications.foreach(a => request.setApplications(a))

    request.setBootstrapActions(flow.bootstrap.map(b => {
      new BootstrapActionConfig(b.name, new ScriptBootstrapActionConfig(b.path, b.args.asJava))
    }) asJava)


    if (! flow.tags.isEmpty) {
      val tags = (flow.tags.map { case (k, v) =>
        new Tag(k, v)
      }) asJava

      request.setTags(tags)
    }

    request.setSteps(toStepConfig(flow.steps) asJava)

    emr.runJobFlow(configureRequest(request)).getJobFlowId
  }

  private def toJobFlowInstancesConfig(config: ClusterConfig, cluster: RunnableCluster, keepAlive: Boolean, terminationProtection: Boolean): JobFlowInstancesConfig = {
    val flow = new JobFlowInstancesConfig

    flow.setEc2KeyName(config.sshKeyPair.orNull)
    flow.setHadoopVersion(config.hadoopVersion.orNull)
    flow.setKeepJobFlowAliveWhenNoSteps(keepAlive)
    flow.setTerminationProtected(terminationProtection)

    for (az <- config.availabilityZone) {
      flow.setPlacement(new PlacementType().withAvailabilityZone(az))
    }

    flow.setEc2SubnetId(config.subnetId.orNull)
    config.subnetIds.foreach { ids => flow.setEc2SubnetIds(ids.asJava) }

    cluster match {
      case traditional: RunnableGroups =>
        val groups = traditional.instances.map(toInstanceGroupConfig(config, _))
        flow.withInstanceGroups(groups asJava)
      case fleet: RunnableFleets =>
        val fleets = fleet.fleets.map(toInstanceFleetConfig(config, _))
        flow.withInstanceFleets(fleets asJava)
    }
  }

  private def toStepConfig(steps: Seq[JobStep]): Seq[StepConfig] = {
    (hiveInstallStepsNeeded(steps) ++ steps).map { s =>
      new StepConfig(s.name, s.toHadoopJarStep).withActionOnFailure(s.actionOnFailure)
    }
  }

  private def hiveInstallStepsNeeded(steps: Seq[JobStep]): Seq[JobStep] = {
    val versions = steps.collect { case h: HiveStep => h.version }

    versions.distinct.map { v => InstallHive(v) }
  }

  private def toInstanceGroupConfig(config: ClusterConfig, instances: InstanceGroup) = {
    val group = new InstanceGroupConfig

    group.setInstanceCount(instances.num)

    group.setInstanceRole(instances match {
      case x: Master => "MASTER"
      case x: Core => "CORE"
      case x: Task => "TASK"
    })

    val size = instances.size.getOrElse(instances match {
      case x: Master => config.defaultMasterSize
      case x: Core => config.defaultCoreSize
      case x: Task => config.defaultTaskSize
    })

    group.setInstanceType(size)

    if (instances.bid.isDefined) {
      group.setMarket("SPOT")
      group.setBidPrice("%2.2f".format(instances.bid.get.bid(size)))
    } else {
      group.setMarket("ON_DEMAND")
    }

    group
  }

  private def toInstanceFleetConfig(config: ClusterConfig, instanceFleet: InstanceFleet) = {
    val fleet = new InstanceFleetConfig
    fleet.setName(instanceFleet.name)
    fleet.setTargetOnDemandCapacity(instanceFleet.onDemandCapacity)
    fleet.setTargetSpotCapacity(instanceFleet.spotCapacity)
    val fleetType = instanceFleet match {
      case _: MasterFleet =>
        InstanceFleetType.MASTER
      case _: TaskFleet =>
        InstanceFleetType.TASK
      case _: CoreFleet =>
        InstanceFleetType.CORE
    }
    fleet.setInstanceFleetType(fleetType)
    instanceFleet.spotSpecification match {
      case Some(spotSpec) =>
        val emrSpotProvisionSpec = new SpotProvisioningSpecification()
        emrSpotProvisionSpec.setBlockDurationMinutes(spotSpec.spotDurationMinutes)
        emrSpotProvisionSpec.setTimeoutAction(spotSpec.timeoutAction)
        emrSpotProvisionSpec.setTimeoutDurationMinutes(spotSpec.timeoutMinutes)
        fleet.setLaunchSpecifications(new InstanceFleetProvisioningSpecifications()
          .withSpotSpecification(emrSpotProvisionSpec))
      case _ =>
    }

    val instanceTypeConfigs = instanceFleet.instances.map { fi =>
      val instanceType = new InstanceTypeConfig()
        .withInstanceType(fi.instanceType)
      if (fi.ebsVolumes.nonEmpty) {
        val ebsConfig = new EbsConfiguration
        ebsConfig.setEbsBlockDeviceConfigs(fi.ebsVolumes.map { volume =>
          val bdConfig = new EbsBlockDeviceConfig
          val volSpec = new VolumeSpecification()
            .withVolumeType(volume.volType)
            .withSizeInGB(volume.gbSize)
          volume.iops.foreach(volSpec.setIops(_))
          bdConfig.setVolumesPerInstance(volume.volumes)
          bdConfig.setVolumeSpecification(volSpec)
          bdConfig
        } asJava)
        instanceType.setEbsConfiguration(ebsConfig)
      }

      instanceType.setWeightedCapacity(instanceFleet.instanceWeighting(fi.instanceType))
      fi.spotPricingStrategy.foreach { strategy =>
        strategy.bidPrice.foreach(instanceType.setBidPrice)
        strategy.bidVsOnDemandPercent.foreach { percent =>
          instanceType.setBidPriceAsPercentageOfOnDemandPrice(percent.toDouble)
        }
      }

      instanceType
    }

    fleet.setInstanceTypeConfigs(instanceTypeConfigs asJava)

    fleet
  }
}

object Emr {

  def apply() = new Emr(EmrJsonCredentials())
  def apply(credentials: AWSCredentials) = new Emr(credentials)
  def run(flow: JobFlow)(implicit config: ClusterConfig): String = {
    apply().run(flow)(config)
  }
}
