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

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreateTopicRequest.TopicDetails

import scala.collection._
import scala.collection.JavaConverters._

class AdminManager(val config: KafkaConfig,
                   val metrics: Metrics,
                   val metadataCache: MetadataCache,
                   val zkUtils: ZkUtils) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Admin Manager on Broker " + config.brokerId + "]: "

  val topicPurgatory = new DelayedOperationPurgatory[DelayedOperation]("topic", config.brokerId)

  /**
    * Try to complete delayed admin topic operations with the request key;
    */
  def tryCompleteDelayedTopicOperations(key: DelayedOperationKey) {
    val completed = topicPurgatory.checkAndComplete(key)
    debug(s"Request key ${key.keyLabel} unblocked $completed admin topic requests.")
  }

  /**
    * Create topics and wait until the topics have been completely created.
    * The callback function will be triggered either when timeout, error or the topics are created.
    */
  def createTopics(timeout: Int,
                   createInfo: Map[String, TopicDetails],
                   responseCallback: Map[String, Errors] => Unit) {

    // 1. map over topics creating assignment and calling zookeeper
    val brokers = metadataCache.getAliveBrokers.map(_.id)
    val metadata = createInfo.map { case (topic, arguments) =>
      try {
        val configs = new Properties()
        arguments.configs.asScala.foreach { case (key, value) =>
          configs.setProperty(key, value)
        }
        LogConfig.validate(configs)

        val assignments = {
          if (!arguments.replicasAssignments.isEmpty) {
            // Note: we don't check that replicaAssignment doesn't contain unknown brokers - unlike in add-partitions case,
            // this follows the existing logic in TopicCommand
            arguments.replicasAssignments.asScala.map { case (partitionId, replicas) =>
              (partitionId.intValue, replicas.asScala.map(_.intValue).toSeq)
            }
          } else {
            AdminUtils.assignReplicasToBrokers(brokers, arguments.partitions, arguments.replicationFactor)
          }
        }
        trace(s"Assignments for topic $topic are $assignments ")
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignments, configs, update = false)
        CreateTopicMetadata(topic, assignments, Errors.NONE)
      } catch {
        case e: Throwable =>
          error(s"Error processing create topic request for topic $topic with arguments $arguments", e)
          CreateTopicMetadata(topic, Map(), Errors.forException(e))
      }
    }

    // 2. if timeout <= 0 or no topics can proceed return immediately
    if (timeout <= 0 || !metadata.exists(_.error == Errors.NONE)) {
      val results = metadata.map { createTopicMetadata =>
        // ignore topics that already have errors
        if (createTopicMetadata.error == Errors.NONE) {
          (createTopicMetadata.topic, Errors.REQUEST_TIMED_OUT)
        } else {
          (createTopicMetadata.topic, createTopicMetadata.error)
        }
      }.toMap
      responseCallback(results)
    } else {
      // 3. else pass the assignments and errors to the delayed operation and set the keys
      val delayedCreate = new DelayedCreateTopics(timeout, metadata.toSeq, this, responseCallback)
      val delayedCreateKeys = createInfo.keys.map(new TopicKey(_)).toSeq
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys)
    }
  }
}
