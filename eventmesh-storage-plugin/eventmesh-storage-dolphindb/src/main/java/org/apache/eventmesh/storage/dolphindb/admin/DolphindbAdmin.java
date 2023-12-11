/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.storage.dolphindb.admin;

import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.api.admin.AbstractAdmin;
import org.apache.eventmesh.api.admin.TopicProperties;
import org.apache.eventmesh.storage.dolphindb.utils.DolphindbUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class DolphindbAdmin extends AbstractAdmin {

    public DolphindbAdmin() {
        super(new AtomicBoolean(false));
    }

    @Override
    public List<TopicProperties> getTopic() throws Exception {

        ConcurrentMap<String, String> topicTableMap = DolphindbUtils.topicAndTableMap;

        return topicTableMap.keySet().stream().map((topic) -> {
            long messageCount = 0;//TODO 怎么从dolphindb获取
            TopicProperties topicProperties = new TopicProperties(topic, messageCount);
            return topicProperties;
        }).collect(Collectors.toList());

    }

    @Override
    public void createTopic(String topicName) {

    }

    @Override
    public void deleteTopic(String topicName) {
          //TODO
    }

    @Override
    public List<CloudEvent> getEvent(String topicName, int offset, int length) throws Exception {

        return null;
    }

    @Override
    public void publish(CloudEvent cloudEvent) throws Exception {
        //TODO
    }
}
