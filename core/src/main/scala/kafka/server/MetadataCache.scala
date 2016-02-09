/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.common.{ReplicaNotAvailableException, LeaderNotAvailableException}

import kafka.api._
import kafka.controller.KafkaController.StateChangeLogger
import kafka.server.MetadataCache.Metadata
import org.apache.kafka.common.{Node, PartitionInfo}
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.UpdateMetadataRequest
import scala.collection.{Seq, Set, mutable}
import kafka.utils.Logging
import kafka.utils.CoreUtils._

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
private[server] class MetadataCache(brokerId: Int) extends Logging {
  this.logIdent = "[Kafka Metadata Cache on broker %d] ".format(brokerId)

  private val metadataLock = new ReentrantReadWriteLock()

  private case class NodeKey(brokerId: Int, securityProtocol: SecurityProtocol)
  private var aliveNodesCache: Map[NodeKey, Node] = Map()

  private val cache: mutable.Map[String, mutable.Map[Int, UpdateMetadataRequest.PartitionState]] =
    new mutable.HashMap[String, mutable.Map[Int, UpdateMetadataRequest.PartitionState]]()

  def getTopicMetadata(topics: Set[String], protocol: SecurityProtocol): Metadata = {
    val errors = mutable.Map[String, Errors]()
    val partitions = mutable.ListBuffer[PartitionInfo]()

    val isAllTopics = topics.isEmpty
    val topicsRequested = if(isAllTopics) cache.keySet else topics

    inReadLock(metadataLock) {
      for (topic <- topicsRequested) {
        if (cache.contains(topic)) {
          cache(topic).foreach { case (partitionId, partitionState) =>
            try {
              val leaderNode = aliveNodesCache.getOrElse(NodeKey(partitionState.leader, protocol),
                  throw new LeaderNotAvailableException(s"Leader not available for [$topic, $partitionId]"))

              val replicaNodes = {
                val replicas = partitionState.replicas.asScala.map(_.toInt)
                val liveReplicas = replicas.filter(id => aliveNodesCache.contains(NodeKey(id, protocol)))
                val missingReplicas = replicas.diff(liveReplicas)

                if (missingReplicas.nonEmpty)
                  throw new ReplicaNotAvailableException(
                    s"Replica information not available for following brokers: ${missingReplicas.mkString(",")}")

                replicas.map { replica => aliveNodesCache(NodeKey(replica, protocol)) }
              }

              val isrNodes = {
                val isr = partitionState.isr.asScala.map(_.toInt)
                val liveIsr = isr.filter(id => aliveNodesCache.contains(NodeKey(id, protocol)))
                val missingIsr = isr.diff(liveIsr)

                if (missingIsr.nonEmpty)
                  throw new ReplicaNotAvailableException(
                    s"In Sync Replica information not available for following brokers:: ${missingIsr.mkString(",")}")

                isr.map { isrReplica => aliveNodesCache(NodeKey(isrReplica, protocol)) }
              }

              partitions += new PartitionInfo(topic, partitionId, leaderNode, replicaNodes.toArray, isrNodes.toArray)
            } catch {
              case e: Throwable =>
                debug(s"Error while fetching metadata for [$topic,$partitionId]", e)
                errors.put(topic, Errors.forException(e))
            }
          }
        } else {
          errors.put(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
      }
    }

    Metadata(partitions, errors)
  }

  def getAliveNodes(protocol: SecurityProtocol): Seq[Node] = {
    inReadLock(metadataLock) {
      aliveNodesCache.filter {
        case (key, _) => key.securityProtocol ==  protocol
      }.values.toSeq
    }
  }

  def getAliveNode(brokerId: Int, protocol: SecurityProtocol): Option[Node] = {
    inReadLock(metadataLock) {
      aliveNodesCache.get(NodeKey(brokerId, protocol))
    }
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataRequest.PartitionState] = {
    inReadLock(metadataLock) {
      cache.get(topic) match {
        case Some(partitionInfos) => partitionInfos.get(partitionId)
        case None => None
      }
    }
  }

  def contains(topic: String): Boolean = {
    inReadLock(metadataLock) {
      cache.contains(topic)
    }
  }

  def updateCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest, brokerId: Int, stateChangeLogger: StateChangeLogger): Unit = {
    inWriteLock(metadataLock) {
      aliveNodesCache = liveBrokersToLiveNodeMap(updateMetadataRequest.liveBrokers.asScala)
      updateMetadataRequest.partitionStates.asScala.foreach { case(tp, info) =>
        if (info.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(tp.topic, tp.partition)
          stateChangeLogger.trace(("Broker %d deleted partition %s from metadata cache in response to UpdateMetadata request " +
            "sent by controller %d epoch %d with correlation id %d")
            .format(brokerId, tp, updateMetadataRequest.controllerId,
              updateMetadataRequest.controllerEpoch, correlationId))
        } else {
          addOrUpdatePartitionInfo(tp.topic, tp.partition, info)
          stateChangeLogger.trace(("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
            "sent by controller %d epoch %d with correlation id %d")
            .format(brokerId, info, tp, updateMetadataRequest.controllerId,
              updateMetadataRequest.controllerEpoch, correlationId))
        }
      }
    }
  }

  private def liveBrokersToLiveNodeMap(liveBrokers: Set[UpdateMetadataRequest.Broker]): Map[NodeKey, Node] = {
    liveBrokers.flatMap { broker =>
      broker.endPoints.asScala.map { case (securityProtocol, endpoint) =>
        val key = new NodeKey(broker.id, securityProtocol)
        val value = new Node(broker.id, endpoint.host, endpoint.port)
        (key, value)
      }
    }.toMap
  }

  private def removePartitionInfo(topic: String, partitionId: Int) = {
    cache.get(topic) match {
      case Some(infos) => {
        infos.remove(partitionId)
        if(infos.isEmpty) {
          cache.remove(topic)
        }
        true
      }
      case None => false
    }
  }

  def addOrUpdatePartitionInfo(topic: String,
                               partitionId: Int,
                               stateInfo: UpdateMetadataRequest.PartitionState) {
    inWriteLock(metadataLock) {
      cache.get(topic) match {
        case Some(infos) => infos.put(partitionId, stateInfo)
        case None => {
          val newInfos = new mutable.HashMap[Int, UpdateMetadataRequest.PartitionState]
          cache.put(topic, newInfos)
          newInfos.put(partitionId, stateInfo)
        }
      }
    }
  }
}

object MetadataCache {
  case class Metadata(partitions: Seq[PartitionInfo], errors: mutable.Map[String, Errors])
}
