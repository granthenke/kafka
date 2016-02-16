/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.*;

public class DeleteTopicRequest extends AbstractRequest {
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.DELETE_TOPIC.id);
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String TIMEOUT_KEY_NAME = "timeout";

    private final Set<String> topics;
    private final Integer timeout;

    public DeleteTopicRequest(Set<String> topics, Integer timeout) {
        super(new Struct(CURRENT_SCHEMA));

        struct.set(TOPICS_KEY_NAME, topics.toArray());
        struct.set(TIMEOUT_KEY_NAME, timeout);

        this.topics = topics;
        this.timeout = timeout;
    }

    public DeleteTopicRequest(Struct struct) {
        super(struct);
        Object[] topicsArray = struct.getArray(TOPICS_KEY_NAME);
        topics = new HashSet<>(topicsArray.length);
        for (Object topic : topicsArray) {
            topics.add((String) topic);
        }
        timeout = struct.getInt(TIMEOUT_KEY_NAME);
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<String, Errors> topicErrors = new HashMap<>();
        for (String topic : topics) {
            topicErrors.put(topic, Errors.forException(e));
        }

        switch (versionId) {
            case 0:
                return new DeleteTopicResponse(topicErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.DELETE_TOPIC.id)));
        }
    }

    public Set<String> topics() {
        return topics;
    }

    public Integer timeout() {
        return this.timeout;
    }

    public static DeleteTopicRequest parse(ByteBuffer buffer, int versionId) {
        return new DeleteTopicRequest(ProtoUtils.parseRequest(ApiKeys.DELETE_TOPIC.id, versionId, buffer));
    }

    public static DeleteTopicRequest parse(ByteBuffer buffer) {
        return new DeleteTopicRequest(CURRENT_SCHEMA.read(buffer));
    }
}
