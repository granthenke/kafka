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

import java.nio.ByteBuffer

import kafka.utils._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DeleteTopicResponse, DeleteTopicRequest, RequestHeader, ResponseHeader}
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

/**
  * TODO: This is a pseudo-temporary test implementation to test DeleteTopicRequestTest while we still do not have an AdminClient.
  * Once the AdminClient is added this should be changed to utilize that instead of this custom/duplicated socket code.
  */
class DeleteTopicRequestTest extends BaseAdminRequestTest {

  @Test
  def testValidDeleteTopicRequests() {
    TestUtils.createTopic(zkUtils, "topic-1", 1, 1, servers)
    validateValidDeleteTopicRequests(new DeleteTopicRequest(Set("topic-1").asJava))

    TestUtils.createTopic(zkUtils, "topic-2", 5, 2, servers)
    TestUtils.createTopic(zkUtils, "topic-3", 1, 2, servers)
    validateValidDeleteTopicRequests(new DeleteTopicRequest(Set("topic-2", "topic-3").asJava))
  }

  private def validateValidDeleteTopicRequests(request: DeleteTopicRequest): Unit = {
    val response = sendDeleteTopicRequest(request)

    val error = response.errors.values.asScala.find(_ != Errors.NONE)
    assertTrue(s"There should be no errors, found ${response.errors.asScala}", error.isEmpty)

    request.topics.asScala.foreach { topic =>
      TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
      assertFalse(s"The topic $topic should not exist", topicExists(topic))
    }
  }

  @Test
  def testInvalidDeleteTopicRequests() {
    // Basic
    validateInvalidDeleteTopicRequests(new DeleteTopicRequest(Set("invalid-topic").asJava),
      Map("invalid-topic" -> Errors.INVALID_TOPIC_EXCEPTION))

    // Partial
    TestUtils.createTopic(zkUtils, "partial-topic-1", 1, 1, servers)
    validateInvalidDeleteTopicRequests(new DeleteTopicRequest(Set(
      "partial-topic-1",
      "partial-invalid-topic").asJava),
      Map(
        "partial-topic-1" -> Errors.NONE,
        "partial-invalid-topic" -> Errors.INVALID_TOPIC_EXCEPTION
      )
    )
  }

  private def validateInvalidDeleteTopicRequests(request: DeleteTopicRequest, expectedResponse: Map[String, Errors]): Unit = {
    val response = sendDeleteTopicRequest(request)
    val errors = response.errors.asScala
    assertEquals("The response size should match", expectedResponse.size, response.errors.size)

    expectedResponse.foreach { case (topic, expectedError) =>
      assertEquals("The response error should match", expectedResponse(topic), errors(topic))
      // If no error validate the topic was deleted
      if (expectedError == Errors.NONE) {
        TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
        assertFalse(s"The topic $topic should not exist", topicExists(topic))
      }
    }
  }

  private def sendDeleteTopicRequest(request: DeleteTopicRequest): DeleteTopicResponse = {
    val correlationId = -1

    val serializedBytes = {
      val header = new RequestHeader(ApiKeys.DELETE_TOPIC.id, 0, "", correlationId)
      val byteBuffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf)
      header.writeTo(byteBuffer)
      request.writeTo(byteBuffer)
      byteBuffer.array()
    }

    val response = requestAndReceive(serializedBytes)

    val responseBuffer = ByteBuffer.wrap(response)
    val responseHeader = ResponseHeader.parse(responseBuffer)
    DeleteTopicResponse.parse(responseBuffer)
  }
}
