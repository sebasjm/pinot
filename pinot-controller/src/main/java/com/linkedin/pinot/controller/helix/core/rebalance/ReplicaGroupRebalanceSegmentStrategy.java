/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.rebalance;

import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignment;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignmentGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Replica group aware rebalance segment strategy.
 */
public class ReplicaGroupRebalanceSegmentStrategy extends BaseRebalanceSegmentStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaGroupRebalanceSegmentStrategy.class);
  private static final boolean DEFAULT_REPLICA_GROUP_BOOTSTRAP = false;

  public ReplicaGroupRebalanceSegmentStrategy(HelixManager helixManager) {
    super(helixManager);
  }

  @Override
  public PartitionAssignment rebalancePartitionAssignment(IdealState idealState, TableConfig tableConfig,
      Configuration rebalanceUserConfig) {
    // Currently, only offline table is supported
    if (tableConfig.getTableType() == CommonConstants.Helix.TableType.REALTIME) {
      throw new UnsupportedOperationException("Replica group rebalancer is currently not supported for Realtime table");
    }

    String tableNameWithType =
        TableNameBuilder.forType(tableConfig.getTableType()).tableNameWithType(tableConfig.getTableName());
    LOGGER.info("Rebalancing replica group partition assignment for table {}", tableNameWithType);

    ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator =
        new ReplicaGroupPartitionAssignmentGenerator(_propertyStore);
    boolean bootstrap = rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.REPLICA_GROUP_BOOTSTRAP,
        DEFAULT_REPLICA_GROUP_BOOTSTRAP);

    // Compute new replica group partition assignment
    ReplicaGroupPartitionAssignment newPartitionAssignment =
        computeNewReplicaGroupMapping(tableConfig, partitionAssignmentGenerator, bootstrap);

    boolean dryRun = rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.DRYRUN, DEFAULT_DRY_RUN);
    if (!dryRun) {
      LOGGER.info("Updating replica group partition assignment for table {}", tableNameWithType);
      partitionAssignmentGenerator.writeReplicaGroupPartitionAssignment(newPartitionAssignment);
    } else {
      LOGGER.info("Dry run. Skip writing replica group partition assignment to property store");
    }

    return newPartitionAssignment;
  }

  @Override
  public IdealState rebalanceIdealState(IdealState idealState, TableConfig tableConfig,
      Configuration rebalanceUserConfig, PartitionAssignment newPartitionAssignment) {

    ReplicaGroupPartitionAssignment newReplicaGroupPartitionAssignment =
        (ReplicaGroupPartitionAssignment) newPartitionAssignment;
    String tableNameWithType =
        TableNameBuilder.forType(tableConfig.getTableType()).tableNameWithType(tableConfig.getTableName());
    boolean bootstrap = rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.REPLICA_GROUP_BOOTSTRAP,
        DEFAULT_REPLICA_GROUP_BOOTSTRAP);

    // Rebalancing segments
    IdealState newIdealState =
        rebalanceSegments(idealState, tableConfig, newReplicaGroupPartitionAssignment, bootstrap);
    int numReplication = tableConfig.getValidationConfig().getReplicationNumber();

    // update if not dryRun
    boolean dryRun = rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.DRYRUN, DEFAULT_DRY_RUN);
    if (!dryRun) {
      LOGGER.info("Updating ideal state for table {}", tableNameWithType);
      updateIdealState(tableNameWithType, numReplication, idealState.getRecord().getMapFields());
    } else {
      LOGGER.info("Dry run. Skip writing ideal state");
    }

    return newIdealState;
  }

  /**
   * Compute the new replica group mapping based on the new configurations
   *
   * @return New server mapping for replica group
   */
  private ReplicaGroupPartitionAssignment computeNewReplicaGroupMapping(TableConfig tableConfig,
      ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator, boolean bootstrap) {
    String tableNameWithType = tableConfig.getTableName();
    ReplicaGroupPartitionAssignment oldReplicaGroupPartitionAssignment =
        partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);

    String serverTenantName = ControllerTenantNameBuilder.getTenantName(tableConfig.getTenantConfig().getServer(),
        tableConfig.getTableType().getServerType());
    List<String> serverInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, serverTenantName);
    ReplicaGroupStrategyConfig replicaGroupConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();

    // If no replica group config is available, we cannot perform the rebalance algorithm
    if (replicaGroupConfig == null || replicaGroupConfig.getPartitionColumn() != null) {
      throw new UnsupportedOperationException(
          "This table is not using replica group segment assignment or table level replica group");
    }

    // If the old replica group partition assignment does not exist, build the new one
    if (bootstrap) {
      return partitionAssignmentGenerator.buildReplicaGroupPartitionAssignment(tableConfig.getTableName(), tableConfig,
          serverInstances);
    }

    if (oldReplicaGroupPartitionAssignment == null) {
      throw new UnsupportedOperationException("Replica group partition assignment does not exist. "
          + "Only boostrapping or rebalancing replica group table is allowed.");
    }

    // Fetch the information required for computing new replica group partition assignment
    int targetNumInstancesPerPartition = replicaGroupConfig.getNumInstancesPerPartition();
    int targetNumReplicaGroup = tableConfig.getValidationConfig().getReplicationNumber();
    int oldNumReplicaGroup = oldReplicaGroupPartitionAssignment.getNumReplicaGroups();
    int oldNumInstancesPerPartition = oldReplicaGroupPartitionAssignment.getAllInstances().size() / oldNumReplicaGroup;

    // Perform the basic validation
    if (targetNumReplicaGroup <= 0 || targetNumInstancesPerPartition <= 0
        || targetNumReplicaGroup * targetNumInstancesPerPartition > serverInstances.size()) {
      throw new UnsupportedOperationException(
          "Unsupported input config (numReplicaGroup: " + targetNumReplicaGroup + ", " + "numInstancesPerPartition: "
              + targetNumInstancesPerPartition + ", numServers: " + serverInstances.size() + ")");
    }

    // Compute added and removed servers
    List<String> oldServerInstances = oldReplicaGroupPartitionAssignment.getAllInstances();
    List<String> addedServers = new ArrayList<>();
    List<String> removedServers = new ArrayList<>();
    for (String server : oldServerInstances) {
      if (!serverInstances.contains(server)) {
        removedServers.add(server);
      }
    }
    for (String server : serverInstances) {
      if (!oldServerInstances.contains(server)) {
        addedServers.add(server);
      }
    }

    // Create the new replica group partition assignment
    ReplicaGroupPartitionAssignment newReplicaGroupPartitionAssignment =
        new ReplicaGroupPartitionAssignment(oldReplicaGroupPartitionAssignment.getTableName());

    // In case of replacement, create the removed to added server mapping
    Map<String, String> oldToNewServerMapping = new HashMap<>();
    if (addedServers.size() == removedServers.size()) {
      for (int i = 0; i < addedServers.size(); i++) {
        oldToNewServerMapping.put(removedServers.get(i), addedServers.get(i));
      }
    }

    ReplicaGroupRebalanceType rebalanceType =
        computeRebalanceType(oldNumInstancesPerPartition, oldNumReplicaGroup, targetNumReplicaGroup,
            targetNumInstancesPerPartition, addedServers, removedServers);

    for (int partitionId = 0; partitionId < oldReplicaGroupPartitionAssignment.getNumPartitions(); partitionId++) {
      for (int groupId = 0; groupId < oldNumReplicaGroup; groupId++) {
        List<String> oldReplicaGroup =
            oldReplicaGroupPartitionAssignment.getInstancesfromReplicaGroup(partitionId, groupId);
        List<String> newReplicaGroup = new ArrayList<>();
        boolean removeGroup = false;
        switch (rebalanceType) {
          case REPLACE:
            // Swap the removed server with the added one.
            for (String oldServer : oldReplicaGroup) {
              if (!oldToNewServerMapping.containsKey(oldServer)) {
                newReplicaGroup.add(oldServer);
              } else {
                newReplicaGroup.add(oldToNewServerMapping.get(oldServer));
              }
            }
            break;
          case ADD_SERVER:
            newReplicaGroup.addAll(oldReplicaGroup);
            // Assign new servers to the replica group
            for (int serverIndex = 0; serverIndex < addedServers.size(); serverIndex++) {
              if (serverIndex % targetNumReplicaGroup == groupId) {
                newReplicaGroup.add(addedServers.get(serverIndex));
              }
            }
            break;
          case REMOVE_SERVER:
            // Only add the servers that are not in the removed list
            for (String oldServer : oldReplicaGroup) {
              if (!removedServers.contains(oldServer)) {
                newReplicaGroup.add(oldServer);
              }
            }
            break;
          case ADD_REPLICA_GROUP:
            // Add all servers for original replica groups and add new replica groups later
            newReplicaGroup.addAll(oldReplicaGroup);
            break;
          case REMOVE_REPLICA_GROUP:
            newReplicaGroup.addAll(oldReplicaGroup);
            // mark the group if this is the replica group that needs to be removed
            if (removedServers.containsAll(oldReplicaGroup)) {
              removeGroup = true;
            }
            break;
          default:
            throw new UnsupportedOperationException("Not supported operation");
        }
        if (!removeGroup) {
          newReplicaGroupPartitionAssignment.setInstancesToReplicaGroup(partitionId, groupId, newReplicaGroup);
        }
      }

      // Adding new replica groups if needed
      int index = 0;
      for (int newGroupId = oldNumReplicaGroup; newGroupId < targetNumReplicaGroup; newGroupId++) {
        List<String> newReplicaGroup = new ArrayList<>();
        while (newReplicaGroup.size() < targetNumInstancesPerPartition) {
          newReplicaGroup.add(addedServers.get(index));
          index++;
        }
        newReplicaGroupPartitionAssignment.setInstancesToReplicaGroup(partitionId, newGroupId, newReplicaGroup);
      }
    }

    return newReplicaGroupPartitionAssignment;
  }

  /**
   * Given the list of added/removed servers and the old and new replica group configurations, compute the
   * type of update (e.g. replace server, add servers to each replica group, add replica groups..)
   * @return the update type
   */
  private ReplicaGroupRebalanceType computeRebalanceType(int oldNumInstancesPerPartition, int oldNumReplicaGroup,
      int targetNumReplicaGroup, int targetNumInstancesPerPartition, List<String> addedServers,
      List<String> removedServers) {
    boolean sameNumInstancesPerPartition = oldNumInstancesPerPartition == targetNumInstancesPerPartition;
    boolean sameNumReplicaGroup = oldNumReplicaGroup == targetNumReplicaGroup;
    boolean isAddedServersSizeZero = addedServers.size() == 0;
    boolean isRemovedServersSizeZero = removedServers.size() == 0;

    if (sameNumInstancesPerPartition && sameNumReplicaGroup && addedServers.size() == removedServers.size()) {
      return ReplicaGroupRebalanceType.REPLACE;
    } else if (sameNumInstancesPerPartition) {
      if (oldNumReplicaGroup < targetNumReplicaGroup && !isAddedServersSizeZero && isRemovedServersSizeZero
          && addedServers.size() % targetNumInstancesPerPartition == 0) {
        return ReplicaGroupRebalanceType.ADD_REPLICA_GROUP;
      } else if (oldNumReplicaGroup > targetNumReplicaGroup && isAddedServersSizeZero && !isRemovedServersSizeZero
          && removedServers.size() % targetNumInstancesPerPartition == 0) {
        return ReplicaGroupRebalanceType.REMOVE_REPLICA_GROUP;
      }
    } else if (sameNumReplicaGroup) {
      if (oldNumInstancesPerPartition < targetNumInstancesPerPartition && isRemovedServersSizeZero
          && !isAddedServersSizeZero && addedServers.size() % targetNumReplicaGroup == 0) {
        return ReplicaGroupRebalanceType.ADD_SERVER;
      } else if (oldNumInstancesPerPartition > targetNumInstancesPerPartition && !isRemovedServersSizeZero
          && isAddedServersSizeZero && removedServers.size() % targetNumReplicaGroup == 0) {
        return ReplicaGroupRebalanceType.REMOVE_SERVER;
      }
    }
    return ReplicaGroupRebalanceType.UNSUPPORTED;
  }

  private IdealState rebalanceSegments(IdealState idealState, TableConfig tableConfig,
      ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment, boolean bootstrap) {
    Map<String, Map<String, String>> segmentToServerMapping = idealState.getRecord().getMapFields();
    Map<String, LinkedList<String>> serverToSegments = buildServerToSegmentMapping(segmentToServerMapping, bootstrap);

    // Compute added and removed servers
    List<String> oldServerInstances = new ArrayList<>(serverToSegments.keySet());
    List<String> serverInstances = replicaGroupPartitionAssignment.getAllInstances();

    List<String> addedServers = new ArrayList<>();
    List<String> removedServers = new ArrayList<>();
    for (String server : oldServerInstances) {
      if (!serverInstances.contains(server)) {
        removedServers.add(server);
      }
    }
    for (String server : serverInstances) {
      if (!oldServerInstances.contains(server)) {
        addedServers.add(server);
      }
    }

    // Add servers to the mapping
    for (String server : addedServers) {
      serverToSegments.put(server, new LinkedList<String>());
    }

    // Remove servers from the mapping
    for (String server : removedServers) {
      serverToSegments.remove(server);
    }

    // Check if rebalance can cause the downtime
    Set<String> segmentsToCover = segmentToServerMapping.keySet();
    Set<String> coveredSegments = new HashSet<>();
    for (Map.Entry<String, LinkedList<String>> entry : serverToSegments.entrySet()) {
      if (!removedServers.contains(entry.getKey())) {
        coveredSegments.addAll(entry.getValue());
      }
    }

    // In case of possible downtime, throw exception unless 'force run' flag is set
    if (!segmentsToCover.equals(coveredSegments)) {
      LOGGER.warn("Segment is lost, this rebalance will cause downtime of the segment.");
    }

    ReplicaGroupStrategyConfig replicaGroupConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    // If no replica group config is available, we cannot perform the rebalance algorithm
    if (replicaGroupConfig == null || replicaGroupConfig.getPartitionColumn() != null) {
      throw new UnsupportedOperationException(
          "This table is not using replica group segment assignment or table level replica group");
    }

    boolean mirrorAssignment = replicaGroupConfig.getMirrorAssignmentAcrossReplicaGroups();
    int numPartitions = replicaGroupPartitionAssignment.getNumPartitions();
    int numReplicaGroups = replicaGroupPartitionAssignment.getNumReplicaGroups();

    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      List<String> referenceReplicaGroup = new ArrayList<>();
      for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
        List<String> serversInReplicaGroup =
            replicaGroupPartitionAssignment.getInstancesfromReplicaGroup(partitionId, replicaId);
        if (replicaId == 0) {
          // We need to keep the first replica group in case of mirroring.
          referenceReplicaGroup.addAll(serversInReplicaGroup);
        } else if (mirrorAssignment) {
          // Copy the segment assignment from the reference replica group
          for (int i = 0; i < serversInReplicaGroup.size(); i++) {
            serverToSegments.put(serversInReplicaGroup.get(i), serverToSegments.get(referenceReplicaGroup.get(i)));
          }
          continue;
        }

        // For the case where we remove servers, we need to add the segments that the removed server contained.
        Set<String> currentCoveredSegments = new HashSet<>();
        for (String server : serversInReplicaGroup) {
          currentCoveredSegments.addAll(serverToSegments.get(server));
        }
        Set<String> segmentsToAdd = new HashSet<>(segmentToServerMapping.keySet());
        segmentsToAdd.removeAll(currentCoveredSegments);
        String server = serversInReplicaGroup.iterator().next();
        serverToSegments.get(server).addAll(segmentsToAdd);

        // Uniformly distribute the segments among servers in a replica group
        rebalanceReplicaGroup(serversInReplicaGroup, serverToSegments);
      }
    }
    return updateIdealStateSegmentMapping(idealState, buildSegmentToServerMapping(serverToSegments), numReplicaGroups);
  }

  /**
   * Uniformly distribute segments across servers in a replica group. It adopts a simple algorithm to keep moving
   * the segment from the server with maximum size to the one with minimum size until all servers are balanced.
   *
   * @param serversInReplicaGroup A list of servers within the same replica group
   * @param serverToSegments A Mapping of servers to their segments
   */
  private void rebalanceReplicaGroup(List<String> serversInReplicaGroup,
      Map<String, LinkedList<String>> serverToSegments) {
    while (!isBalanced(serversInReplicaGroup, serverToSegments)) {
      String minKey = serversInReplicaGroup.get(0);
      String maxKey = minKey;
      int minSize = serverToSegments.get(minKey).size();
      int maxSize = minSize;

      for (int i = 1; i < serversInReplicaGroup.size(); i++) {
        String server = serversInReplicaGroup.get(i);
        int currentSegmentSize = serverToSegments.get(server).size();
        if (minSize > currentSegmentSize) {
          minSize = currentSegmentSize;
          minKey = server;
        }
        if (maxSize < currentSegmentSize) {
          maxSize = currentSegmentSize;
          maxKey = server;
        }
      }

      if (!maxKey.equals(minKey)) {
        String segmentToMove = serverToSegments.get(maxKey).poll();
        serverToSegments.get(minKey).push(segmentToMove);
      }
    }
  }

  /**
   * Helper method to check if segments in a replica group is balanced
   *
   * @param serversInReplicaGroup A list of servers for a replica group
   * @param serverToSegments A mapping of server to its segments list
   * @return True if the servers are balanced. False otherwise
   */
  private boolean isBalanced(List<String> serversInReplicaGroup, Map<String, LinkedList<String>> serverToSegments) {
    Set<Integer> numSegments = new HashSet<>();
    for (String server : serversInReplicaGroup) {
      numSegments.add(serverToSegments.get(server).size());
    }
    if (numSegments.size() == 2) {
      Iterator<Integer> iter = numSegments.iterator();
      int first = iter.next();
      int second = iter.next();
      if (Math.abs(first - second) < 2) {
        return true;
      }
    } else if (numSegments.size() < 2) {
      return true;
    }
    return false;
  }

  private Map<String, LinkedList<String>> buildServerToSegmentMapping(
      Map<String, Map<String, String>> segmentToServerMapping, boolean bootstrap) {
    Map<String, LinkedList<String>> serverToSegments = new HashMap<>();
    for (String segment : segmentToServerMapping.keySet()) {
      for (String server : segmentToServerMapping.get(segment).keySet()) {
        if (!serverToSegments.containsKey(server)) {
          serverToSegments.put(server, new LinkedList<String>());
        }
        if (!bootstrap) {
          serverToSegments.get(server).add(segment);
        }
      }
    }
    return serverToSegments;
  }

  private Map<String, Map<String, String>> buildSegmentToServerMapping(
      Map<String, LinkedList<String>> serverToSegments) {
    Map<String, Map<String, String>> segmentsToServerMapping = new HashMap<>();
    for (Map.Entry<String, LinkedList<String>> entry : serverToSegments.entrySet()) {
      String server = entry.getKey();
      for (String segment : entry.getValue()) {
        if (!segmentsToServerMapping.containsKey(segment)) {
          segmentsToServerMapping.put(segment, new HashMap<String, String>());
        }
        segmentsToServerMapping.get(segment)
            .put(server, CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      }
    }
    return segmentsToServerMapping;
  }

  private IdealState updateIdealStateSegmentMapping(IdealState idealState,
      Map<String, Map<String, String>> serverToSegmentsMapping, int numReplica) {
    for (Map.Entry<String, Map<String, String>> entry : serverToSegmentsMapping.entrySet()) {
      idealState.setInstanceStateMap(entry.getKey(), entry.getValue());
    }
    idealState.setReplicas(Integer.toString(numReplica));
    return idealState;
  }

  public enum ReplicaGroupRebalanceType {
    REPLACE, ADD_SERVER, ADD_REPLICA_GROUP, REMOVE_SERVER, REMOVE_REPLICA_GROUP, UNSUPPORTED
  }
}
