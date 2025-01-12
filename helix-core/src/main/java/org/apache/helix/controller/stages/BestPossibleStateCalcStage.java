package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.CustomRebalancer;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.MaintenanceRebalancer;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.ResourceMonitor;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For partition compute best possible (instance,state) pair based on
 * IdealState,StateModel,LiveInstance
 */
public class BestPossibleStateCalcStage extends AbstractBaseStage {
  private static final Logger logger =
      LoggerFactory.getLogger(BestPossibleStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE.name());
    final Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    final ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

    if (currentStateOutput == null || resourceMap == null || cache == null) {
      throw new StageException(
          "Missing attributes in event:" + event + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }

    final BestPossibleStateOutput bestPossibleStateOutput =
        compute(event, resourceMap, currentStateOutput);

    // Add swap-in instances to bestPossibleStateOutput.
    // We do this after computing the best possible state output because rebalance algorithms should not
    // to be aware of swap-in instances. We simply add the swap-in instances to the
    // stateMap where the swap-out instance is and compute the correct state.
    addSwapInInstancesToBestPossibleState(resourceMap, bestPossibleStateOutput, cache);

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);

    final Map<String, InstanceConfig> allInstanceConfigMap = cache.getInstanceConfigMap();
    final Map<String, StateModelDefinition> stateModelDefMap = cache.getStateModelDefMap();
    final Map<String, IdealState> idealStateMap = cache.getIdealStates();
    final Map<String, ExternalView> externalViewMap = cache.getExternalViews();
    final Map<String, ResourceConfig> resourceConfigMap = cache.getResourceConfigMap();
    asyncExecute(cache.getAsyncTasksThreadPool(), () -> {
      try {
        if (clusterStatusMonitor != null) {
          clusterStatusMonitor.setPerInstanceResourceStatus(bestPossibleStateOutput,
              allInstanceConfigMap, resourceMap,
                  stateModelDefMap);

          for (String resourceName : idealStateMap.keySet()) {
            // TODO need to find a better way to process this monitoring config in a centralized
            // place instead of separately in every single usage.
            // Note that it is currently not respected by all the components. This may lead to
            // frequent MBean objects creation and deletion.
            if (resourceConfigMap.containsKey(resourceName) && resourceConfigMap.get(resourceName)
                .isMonitoringDisabled()) {
              continue;
            }
            IdealState is = idealStateMap.get(resourceName);
            reportResourceState(clusterStatusMonitor, bestPossibleStateOutput, resourceName, is,
                externalViewMap.get(resourceName), stateModelDefMap.get(is.getStateModelDefRef()));
          }
        }
      } catch (Exception e) {
        LogUtil.logError(logger, _eventId, "Could not update cluster status metrics!", e);
      }
      return null;
    });
  }

  private void addSwapInInstancesToBestPossibleState(Map<String, Resource> resourceMap,
      BestPossibleStateOutput bestPossibleStateOutput, ResourceControllerDataProvider cache) {
    // 1. Get all SWAP_OUT instances and corresponding SWAP_IN instance pairs in the cluster.
    Map<String, String> swapOutToSwapInInstancePairs = cache.getSwapOutToSwapInInstancePairs();
    // 2. Get all enabled and live SWAP_IN instances in the cluster.
    Set<String> enabledLiveSwapInInstances = cache.getEnabledLiveSwapInInstanceNames();
    // 3. For each SWAP_OUT instance in any of the preferenceLists, add the corresponding SWAP_IN instance to the end.
    // Skipping this when there are not SWAP_IN instances ready(enabled and live) will reduce computation time when there is not an active
    // swap occurring.
    if (!enabledLiveSwapInInstances.isEmpty() && !cache.isMaintenanceModeEnabled()) {
      resourceMap.forEach((resourceName, resource) -> {
        StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());
        bestPossibleStateOutput.getResourceStatesMap().get(resourceName).getStateMap()
            .forEach((partition, stateMap) -> {
              Set<String> commonInstances = new HashSet<>(stateMap.keySet());
              commonInstances.retainAll(swapOutToSwapInInstancePairs.keySet());

              commonInstances.forEach(swapOutInstance -> {
                if (stateMap.get(swapOutInstance).equals(stateModelDef.getTopState())) {

                  String topStateCount =
                      stateModelDef.getNumInstancesPerState(stateModelDef.getTopState());
                  if (topStateCount.equals(
                      StateModelDefinition.STATE_REPLICA_COUNT_ALL_CANDIDATE_NODES)
                      || topStateCount.equals(
                      StateModelDefinition.STATE_REPLICA_COUNT_ALL_REPLICAS)) {
                    // If the swap-out instance's replica is a topState and the StateModel allows for
                    // another replica with the topState to be added, set the swap-in instance's replica
                    // to the topState.
                    stateMap.put(swapOutToSwapInInstancePairs.get(swapOutInstance),
                        stateModelDef.getTopState());
                  } else {
                    // If the swap-out instance's replica is a topState and the StateModel does not allow for
                    // another replica with the topState to be added, set the swap-in instance's replica
                    // to the secondTopState.
                    stateMap.put(swapOutToSwapInInstancePairs.get(swapOutInstance),
                        stateModelDef.getSecondTopStates().iterator().next());
                  }
                } else if (stateModelDef.getSecondTopStates()
                    .contains(stateMap.get(swapOutInstance))) {
                  // If the swap-out instance's replica is a secondTopState, set the swap-in instance's replica
                  // to the same secondTopState.
                  stateMap.put(swapOutToSwapInInstancePairs.get(swapOutInstance),
                      stateMap.get(swapOutInstance));
                }
              });
            });
      });
    }
  }

  private void reportResourceState(ClusterStatusMonitor clusterStatusMonitor,
      BestPossibleStateOutput bestPossibleStateOutput, String resourceName, IdealState is,
      ExternalView ev, StateModelDefinition stateModelDef) {
    // Create a temporary local IdealState object for monitoring. This is to avoid modifying
    // the IdealState cache.
    IdealState tmpIdealState = new IdealState(is.getRecord());

    if (bestPossibleStateOutput.containsResource(resourceName)) {
      // Merge the best possible state output for resource status monitoring.
      Map<String, List<String>> preferenceLists =
          bestPossibleStateOutput.getPreferenceLists(resourceName);
      tmpIdealState.getRecord().setListFields(preferenceLists);
      Map<Partition, Map<String, String>> stateMap =
          bestPossibleStateOutput.getPartitionStateMap(resourceName).getStateMap();
      tmpIdealState.getRecord().setMapFields(stateMap.entrySet().stream()
          .collect(Collectors.toMap(e -> e.getKey().getPartitionName(), Map.Entry::getValue)));
    } else {
      LogUtil.logWarn(logger, _eventId, String.format(
          "Cannot find the best possible state of resource %s. "
              + "Will update the resource status based on the content of the IdealState.",
          resourceName));
    }
    clusterStatusMonitor.setResourceState(resourceName, ev, tmpIdealState, stateModelDef);
  }

  private BestPossibleStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    BestPossibleStateOutput output = new BestPossibleStateOutput();

    HelixManager helixManager = event.getAttribute(AttributeName.helixmanager.name());
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    WagedRebalancer wagedRebalancer = event.getAttribute(AttributeName.STATEFUL_REBALANCER.name());

    // Check whether the offline/disabled instance count in the cluster exceeds the set limit,
    // if yes, put the cluster into maintenance mode.
    boolean isValid =
        validateOfflineInstancesLimit(cache, event.getAttribute(AttributeName.helixmanager.name()));

    final List<String> failureResources = new ArrayList<>();

    Map<String, Resource> calculatedResourceMap =
        computeResourceBestPossibleStateWithWagedRebalancer(wagedRebalancer, cache,
            currentStateOutput, resourceMap, output, failureResources);

    Map<String, Resource> remainingResourceMap = new HashMap<>(resourceMap);
    remainingResourceMap.keySet().removeAll(calculatedResourceMap.keySet());

    // Fallback to the original single resource rebalancer calculation.
    // This is required because we support mixed cluster that uses both WAGED rebalancer and the
    // older rebalancers.
    Iterator<Resource> itr = remainingResourceMap.values().iterator();
    while (itr.hasNext()) {
      Resource resource = itr.next();
      boolean result = false;
      try {
        result = computeSingleResourceBestPossibleState(event, cache, currentStateOutput, resource,
            output);
      } catch (HelixException ex) {
        LogUtil.logError(logger, _eventId, String
            .format("Exception when calculating best possible states for %s",
                resource.getResourceName()), ex);

      }
      if (!result) {
        failureResources.add(resource.getResourceName());
        LogUtil.logWarn(logger, _eventId, String
            .format("Failed to calculate best possible states for %s", resource.getResourceName()));
      }
    }

    // Check and report if resource rebalance has failure
    updateRebalanceStatus(!isValid || !failureResources.isEmpty(), failureResources, helixManager,
        cache, clusterStatusMonitor, String
            .format("Failed to calculate best possible states for %d resources.",
                failureResources.size()));

    return output;
  }

  private void updateRebalanceStatus(final boolean hasFailure, final List<String> failedResources,
      final HelixManager helixManager, final ResourceControllerDataProvider cache,
      final ClusterStatusMonitor clusterStatusMonitor, final String errorMessage) {
    asyncExecute(cache.getAsyncTasksThreadPool(), new Callable<Object>() {
      @Override
      public Object call() {
        try {
          if (hasFailure) {
            /* TODO Enable this update when we resolve ZK server load issue. This will cause extra write to ZK.
            if (_statusUpdateUtil != null) {
              _statusUpdateUtil
                  .logError(StatusUpdateUtil.ErrorType.RebalanceResourceFailure, this.getClass(),
                      errorMessage, helixManager);
            }
            */
            LogUtil.logWarn(logger, _eventId, errorMessage);
          }
          if (clusterStatusMonitor != null) {
            clusterStatusMonitor.setRebalanceFailureGauge(hasFailure);
            clusterStatusMonitor.setResourceRebalanceStates(failedResources,
                ResourceMonitor.RebalanceStatus.BEST_POSSIBLE_STATE_CAL_FAILED);
          }
        } catch (Exception e) {
          LogUtil.logError(logger, _eventId, "Could not update cluster status!", e);
        }
        return null;
      }
    });
  }

  // Check whether the offline/disabled instance count in the cluster reaches the set limit,
  // if yes, auto enable maintenance mode, and use the maintenance rebalancer for this pipeline.
  private boolean validateOfflineInstancesLimit(final ResourceControllerDataProvider cache,
      final HelixManager manager) {
    int maxOfflineInstancesAllowed = cache.getClusterConfig().getMaxOfflineInstancesAllowed();
    if (maxOfflineInstancesAllowed >= 0) {
      int offlineCount =
          cache.getAssignableInstances().size() - cache.getAssignableEnabledLiveInstances().size();
      if (offlineCount > maxOfflineInstancesAllowed) {
        String errMsg = String.format(
            "Offline Instances count %d greater than allowed count %d. Put cluster %s into "
                + "maintenance mode.",
            offlineCount, maxOfflineInstancesAllowed, cache.getClusterName());
        if (manager != null) {
          if (manager.getHelixDataAccessor()
              .getProperty(manager.getHelixDataAccessor().keyBuilder().maintenance()) == null) {
            manager.getClusterManagmentTool()
                .autoEnableMaintenanceMode(manager.getClusterName(), true, errMsg,
                    MaintenanceSignal.AutoTriggerReason.MAX_OFFLINE_INSTANCES_EXCEEDED);
            LogUtil.logWarn(logger, _eventId, errMsg);
          }
        } else {
          LogUtil.logError(logger, _eventId, "Failed to put cluster " + cache.getClusterName()
              + " into maintenance mode, HelixManager is not set!");
        }

        // Enable maintenance mode in cache so the maintenance rebalancer is used for this pipeline
        cache.enableMaintenanceMode();

        return false;
      }
    }
    return true;
  }

  private void updateWagedRebalancer(WagedRebalancer wagedRebalancer, ClusterConfig clusterConfig) {
    if (clusterConfig != null) {
      // Since the rebalance configuration can be updated at runtime, try to update the rebalancer
      // before calculating.
      wagedRebalancer.updateRebalancePreference(clusterConfig.getGlobalRebalancePreference());
      wagedRebalancer
          .setGlobalRebalanceAsyncMode(clusterConfig.isGlobalRebalanceAsyncModeEnabled());
    }
  }

  /**
   * Rebalance with the WAGED rebalancer
   * The rebalancer only calculates the new ideal assignment for all the resources that are
   * configured to use the WAGED rebalancer.
   *
   * @param wagedRebalancer    The WAGED rebalancer instance.
   * @param cache              Cluster data cache.
   * @param currentStateOutput The current state information.
   * @param resourceMap        The complete resource map. The method will filter the map for the compatible resources.
   * @param output             The best possible state output.
   * @param failureResources   The failure records that will be updated if any resource cannot be computed.
   * @return The map of all the calculated resources.
   */
  private Map<String, Resource> computeResourceBestPossibleStateWithWagedRebalancer(
      WagedRebalancer wagedRebalancer, ResourceControllerDataProvider cache,
      CurrentStateOutput currentStateOutput, Map<String, Resource> resourceMap,
      BestPossibleStateOutput output, List<String> failureResources) {
    if (cache.isMaintenanceModeEnabled()) {
      // The WAGED rebalancer won't be used while maintenance mode is enabled.
      return Collections.emptyMap();
    }

    // Find the compatible resources: 1. FULL_AUTO 2. Configured to use the WAGED rebalancer
    Map<String, Resource> wagedRebalancedResourceMap = resourceMap.entrySet().stream()
        .filter(resourceEntry ->
            WagedValidationUtil.isWagedEnabled(cache.getIdealState(resourceEntry.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<String, IdealState> newIdealStates = new HashMap<>();

    if (wagedRebalancer != null) {
      updateWagedRebalancer(wagedRebalancer, cache.getClusterConfig());
      try {
        newIdealStates.putAll(wagedRebalancer
            .computeNewIdealStates(cache, wagedRebalancedResourceMap, currentStateOutput));
      } catch (HelixRebalanceException ex) {
        // Note that unlike the legacy rebalancer, the WAGED rebalance won't return partial result.
        // Since it calculates for all the eligible resources globally, a partial result is invalid.
        // TODO propagate the rebalancer failure information to updateRebalanceStatus for monitoring.
        LogUtil.logError(logger, _eventId, String
            .format("Failed to calculate the new Ideal States using the rebalancer %s due to %s",
                wagedRebalancer.getClass().getSimpleName(), ex.getFailureType()), ex);
      }
    } else {
      LogUtil.logWarn(logger, _eventId,
          "Skip rebalancing using the WAGED rebalancer since it is not configured in the rebalance pipeline.");
    }

    for (Resource resource : wagedRebalancedResourceMap.values()) {
      IdealState is = newIdealStates.get(resource.getResourceName());
      // Check if the WAGED rebalancer has calculated the result for this resource or not.
      if (is != null && checkBestPossibleStateCalculation(is)) {
        // The WAGED rebalancer calculates a valid result, record in the output
        updateBestPossibleStateOutput(output, resource, is);
      } else {
        failureResources.add(resource.getResourceName());
        LogUtil.logWarn(logger, _eventId,
            String.format("The calculated best possible states for %s is empty or invalid.",
                resource.getResourceName()));
      }
    }
    return wagedRebalancedResourceMap;
  }

  private void updateBestPossibleStateOutput(BestPossibleStateOutput output, Resource resource,
      IdealState computedIdealState) {
    output.setPreferenceLists(resource.getResourceName(), computedIdealState.getPreferenceLists());
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> newStateMap =
          computedIdealState.getInstanceStateMap(partition.getPartitionName());
      output.setState(resource.getResourceName(), partition, newStateMap);
    }
  }

  private boolean computeSingleResourceBestPossibleState(ClusterEvent event,
      ResourceControllerDataProvider cache, CurrentStateOutput currentStateOutput,
      Resource resource, BestPossibleStateOutput output) {
    // for each ideal state
    // read the state model def
    // for each resource
    // get the preference list
    // for each instanceName check if its alive then assign a state

    String resourceName = resource.getResourceName();
    LogUtil.logDebug(logger, _eventId, "Processing resource:" + resourceName);
    // Ideal state may be gone. In that case we need to get the state model name
    // from the current state
    IdealState idealState = cache.getIdealState(resourceName);
    if (idealState == null) {
      // if ideal state is deleted, use an empty one
      LogUtil.logInfo(logger, _eventId, "resource:" + resourceName + " does not exist anymore");
      idealState = new IdealState(resourceName);
      idealState.setStateModelDefRef(resource.getStateModelDefRef());
    }

    // Skip resources are tasks for regular pipeline
    if (idealState.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)) {
      LogUtil.logWarn(logger, _eventId, String
          .format("Resource %s should not be processed by %s pipeline", resourceName,
              cache.getPipelineName()));
      return false;
    }

    Rebalancer<ResourceControllerDataProvider> rebalancer =
        getRebalancer(idealState, resourceName, cache.isMaintenanceModeEnabled());
    MappingCalculator<ResourceControllerDataProvider> mappingCalculator =
        getMappingCalculator(rebalancer, resourceName);

    if (rebalancer == null) {
      LogUtil.logError(logger, _eventId, "Error computing assignment for resource " + resourceName
          + ". no rebalancer found. rebalancer: " + rebalancer + " mappingCalculator: "
          + mappingCalculator);
    } else {
      try {
        HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
        rebalancer.init(manager);
        idealState =
            rebalancer.computeNewIdealState(resourceName, idealState, currentStateOutput, cache);

        // Check if calculation is done successfully
        if (!checkBestPossibleStateCalculation(idealState)) {
          LogUtil.logWarn(logger, _eventId,
              "The calculated idealState is not valid, resource: " + resourceName);
          return false;
        }

        // Use the internal MappingCalculator interface to compute the final assignment
        // The next release will support rebalancers that compute the mapping from start to finish
        ResourceAssignment partitionStateAssignment = mappingCalculator
            .computeBestPossiblePartitionState(cache, idealState, resource, currentStateOutput);

        if (partitionStateAssignment == null) {
          LogUtil.logWarn(logger, _eventId,
              "The calculated partitionStateAssignment is null, resource: " + resourceName);
          return false;
        }

        // Set the calculated result to the output.
        output.setPreferenceLists(resourceName, idealState.getPreferenceLists());
        for (Partition partition : resource.getPartitions()) {
          Map<String, String> newStateMap = partitionStateAssignment.getReplicaMap(partition);
          output.setState(resourceName, partition, newStateMap);
        }

        return true;
      } catch (HelixException e) {
        // No eligible instance is found.
        LogUtil.logError(logger, _eventId, e.getMessage());
      } catch (Exception e) {
        LogUtil.logError(logger, _eventId,
            "Error computing assignment for resource " + resourceName + ". Skipping." , e);
      }
    }
    // Exception or rebalancer is not found
    return false;
  }

  private boolean checkBestPossibleStateCalculation(IdealState idealState) {
    // If replicas is 0, indicate the resource is not fully initialized or ready to be rebalanced
    if (idealState.getRebalanceMode() == IdealState.RebalanceMode.FULL_AUTO && !idealState
        .getReplicas().equals("0")) {
      Map<String, List<String>> preferenceLists = idealState.getPreferenceLists();
      if (preferenceLists == null || preferenceLists.isEmpty()) {
        return false;
      }
      int emptyListCount = 0;
      for (List<String> preferenceList : preferenceLists.values()) {
        if (preferenceList.isEmpty()) {
          emptyListCount++;
        }
      }
      // If all lists are empty, rebalance fails completely
      return emptyListCount != preferenceLists.values().size();
    } else {
      // For non FULL_AUTO RebalanceMode, rebalancing is not controlled by Helix
      return true;
    }
  }

  private Rebalancer<ResourceControllerDataProvider> getCustomizedRebalancer(
      String rebalancerClassName, String resourceName) {
    Rebalancer<ResourceControllerDataProvider> customizedRebalancer = null;
    if (rebalancerClassName != null) {
      if (logger.isDebugEnabled()) {
        LogUtil.logDebug(logger, _eventId,
            "resource " + resourceName + " use idealStateRebalancer " + rebalancerClassName);
      }
      try {
        customizedRebalancer = Rebalancer.class
            .cast(HelixUtil.loadClass(getClass(), rebalancerClassName).newInstance());
      } catch (Exception e) {
        LogUtil.logError(logger, _eventId,
            "Exception while invoking custom rebalancer class:" + rebalancerClassName, e);
      }
    }
    return customizedRebalancer;
  }

  private Rebalancer<ResourceControllerDataProvider> getRebalancer(IdealState idealState,
      String resourceName, boolean isMaintenanceModeEnabled) {
    Rebalancer<ResourceControllerDataProvider> rebalancer = null;
    switch (idealState.getRebalanceMode()) {
    case FULL_AUTO:
      if (isMaintenanceModeEnabled) {
        rebalancer = new MaintenanceRebalancer();
      } else {
        Rebalancer<ResourceControllerDataProvider> customizedRebalancer =
            getCustomizedRebalancer(idealState.getRebalancerClassName(), resourceName);
        if (customizedRebalancer != null) {
          rebalancer = customizedRebalancer;
        } else {
          rebalancer = new DelayedAutoRebalancer();
        }
      }
      break;
    case SEMI_AUTO:
      rebalancer = new SemiAutoRebalancer<>();
      break;
    case CUSTOMIZED:
      rebalancer = new CustomRebalancer();
      break;
    case USER_DEFINED:
    case TASK:
      rebalancer = getCustomizedRebalancer(idealState.getRebalancerClassName(), resourceName);
      break;
    default:
      LogUtil.logError(logger, _eventId,
          "Fail to find the rebalancer, invalid rebalance mode " + idealState.getRebalanceMode());
      break;
    }
    return rebalancer;
  }

  private MappingCalculator<ResourceControllerDataProvider> getMappingCalculator(
      Rebalancer<ResourceControllerDataProvider> rebalancer, String resourceName) {
    MappingCalculator<ResourceControllerDataProvider> mappingCalculator = null;

    if (rebalancer != null) {
      try {
        mappingCalculator = MappingCalculator.class.cast(rebalancer);
      } catch (ClassCastException e) {
        LogUtil.logWarn(logger, _eventId,
            "Rebalancer does not have a mapping calculator, defaulting to SEMI_AUTO, resource: "
                + resourceName);
      }
    }
    if (mappingCalculator == null) {
      mappingCalculator = new SemiAutoRebalancer<>();
    }

    return mappingCalculator;
  }
}
