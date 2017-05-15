package com.bizo.hive.sparkplug.emr

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce
import com.amazonaws.services.elasticmapreduce.model.{Unit => EMRUnit, _}
import com.amazonaws.ResponseMetadata
import com.amazonaws.AmazonWebServiceRequest
import java.util.UUID

import com.amazonaws.regions.Region
import com.amazonaws.services.elasticmapreduce.waiters.AmazonElasticMapReduceWaiters

class TestAmazonElasticMapReduceClient extends AmazonElasticMapReduce {
  var jobFlowRequest: RunJobFlowRequest = null

  def runJobFlow(runJobFlowRequest: RunJobFlowRequest): RunJobFlowResult = {

    jobFlowRequest = runJobFlowRequest

    new RunJobFlowResult().withJobFlowId(UUID.randomUUID.toString)
  }

  def addInstanceGroups(addInstanceGroupsRequest: AddInstanceGroupsRequest): AddInstanceGroupsResult = ???
  def addJobFlowSteps(addJobFlowStepsRequest: AddJobFlowStepsRequest) = ???
  def terminateJobFlows(terminateJobFlowsRequest: TerminateJobFlowsRequest) = ???
  def describeJobFlows(describeJobFlowsRequest: DescribeJobFlowsRequest): DescribeJobFlowsResult = ???
  def setTerminationProtection(setTerminationProtectionRequest: SetTerminationProtectionRequest) = ???
  def modifyInstanceGroups(modifyInstanceGroupsRequest: ModifyInstanceGroupsRequest) = ???
  def describeJobFlows(): DescribeJobFlowsResult = ???
  def modifyInstanceGroups() = ???
  def shutdown() = ???
  def setEndpoint(endpoint: String) = ???
  def getCachedResponseMetadata(request: AmazonWebServiceRequest): ResponseMetadata = ???
  def setVisibleToAllUsers(visibleToAllUsers: SetVisibleToAllUsersRequest) = ???
  def describeCluster(describeClusterRequest: DescribeClusterRequest): DescribeClusterResult = ???
  def describeStep(describeStepRequest: DescribeStepRequest): DescribeStepResult = ???
  def listBootstrapActions(listBootstrapActionsRequest: ListBootstrapActionsRequest): ListBootstrapActionsResult = ???
  def listClusters(): ListClustersResult = ???
  def listClusters(listClustersRequest: ListClustersRequest): ListClustersResult = ???
  def listInstanceGroups(listInstanceGroupsRequest: ListInstanceGroupsRequest): ListInstanceGroupsResult = ???
  def listInstances(listInstancesRequest: ListInstancesRequest): ListInstancesResult = ???
  def listSteps(listStepsRequest: ListStepsRequest): ListStepsResult = ???
  def addTags(addTagsRequest: AddTagsRequest): AddTagsResult = ???
  def removeTags(): RemoveTagsResult = ???
  def removeTags(x$1: RemoveTagsRequest): RemoveTagsResult = ???
  def setRegion(region: Region): Unit = ???
  def deleteSecurityConfiguration(deleteSecurityConfigurationRequest: DeleteSecurityConfigurationRequest): DeleteSecurityConfigurationResult = ???
  def listInstanceFleets(listInstanceFleetsRequest: ListInstanceFleetsRequest): ListInstanceFleetsResult = ???
  def modifyInstanceFleet(modifyInstanceFleetRequest: ModifyInstanceFleetRequest): ModifyInstanceFleetResult = ???
  def waiters(): AmazonElasticMapReduceWaiters = ???
  def putAutoScalingPolicy(putAutoScalingPolicyRequest: PutAutoScalingPolicyRequest): PutAutoScalingPolicyResult = ???
  def listSecurityConfigurations(listSecurityConfigurationsRequest: ListSecurityConfigurationsRequest): ListSecurityConfigurationsResult = ???
  def describeSecurityConfiguration(describeSecurityConfigurationRequest: DescribeSecurityConfigurationRequest): DescribeSecurityConfigurationResult = ???
  def cancelSteps(cancelStepsRequest: CancelStepsRequest): CancelStepsResult = ???
  def addInstanceFleet(addInstanceFleetRequest: AddInstanceFleetRequest): AddInstanceFleetResult = ???
  def removeAutoScalingPolicy(removeAutoScalingPolicyRequest: RemoveAutoScalingPolicyRequest): RemoveAutoScalingPolicyResult = ???
  def createSecurityConfiguration(createSecurityConfigurationRequest: CreateSecurityConfigurationRequest): CreateSecurityConfigurationResult = ???
}