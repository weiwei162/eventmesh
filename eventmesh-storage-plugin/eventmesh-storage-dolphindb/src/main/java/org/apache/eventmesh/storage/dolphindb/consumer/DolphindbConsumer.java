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

package org.apache.eventmesh.storage.dolphindb.consumer;

import com.google.common.base.Preconditions;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.StreamDeserializer;
import com.xxdb.streaming.client.ThreadedClient;
import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.api.AbstractContext;
import org.apache.eventmesh.api.EventListener;
import org.apache.eventmesh.api.consumer.Consumer;
import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.storage.dolphindb.client.DolphindbSubscribeClient;
import org.apache.eventmesh.storage.dolphindb.config.DolphindbConfiguration;
import org.apache.eventmesh.storage.dolphindb.streaming.DolphindbMessageHandler;
import org.apache.eventmesh.storage.dolphindb.utils.DolphindbUtils;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费者
 */
@Slf4j
public class DolphindbConsumer implements Consumer {

    //private ThreadPooledClient subscribePoolClient;
    private DolphindbMessageHandler dolphindbMessageHandler;

    private DolphindbConfiguration dolphindbConfig;
    private final AtomicBoolean isStarted;

    public DolphindbConsumer(Properties properties) {
       this.isStarted = new AtomicBoolean(false);
    }

    @Override
    public boolean isStarted() {
        return isStarted.get();
    }

    @Override
    public boolean isClosed() {
        return !isStarted.get();
    }

    @Override
    public void start() {
        isStarted.compareAndSet(false, true);
    }

    @Override
    public void shutdown() {
        if (isStarted.get()){
            //subscribePoolClient = null;
            dolphindbMessageHandler = null;
        }
        isStarted.compareAndSet(true, false);
    }

    @Override
    public void init(Properties keyValue) throws Exception {
        //subscribePoolClient = DolphindbSubscribeClient.INSTANCE;
        ConfigService configService = ConfigService.getInstance();
        dolphindbConfig = configService.buildConfigInstance(DolphindbConfiguration.class);
    }

    @Override
    public void updateOffset(List<CloudEvent> cloudEvents, AbstractContext context) {

    }

    @Override
    public void subscribe(String topic) throws Exception {

        String host = dolphindbConfig.getHost();
        Integer port = dolphindbConfig.getPort();
        String tableName = DolphindbUtils.getTopicTableName(topic);
        String actionName = tableName;//先使用表名称 TODO
        Long offset = dolphindbConfig.getOffset();//TODO 需要计算
        Boolean reconnect = dolphindbConfig.getReconnect();
        Vector filter = null;
        StreamDeserializer deserializer = null;
        Boolean allowExistTopic = false; //TODO
        String user = dolphindbConfig.getUser();
        String password = dolphindbConfig.getPassword();
        String key = topic + "_" + actionName;
        if (StringUtils.isNotEmpty(tableName)) {//主题存在且获取到发布表tableName名称

            //检查订阅，获取历史订阅offset位置 TODO

            //开始订阅
            ThreadedClient threadedClient = DolphindbSubscribeClient.getThreadedClient(key);
            threadedClient.subscribe(host, port, tableName, actionName, dolphindbMessageHandler, offset, reconnect,
                    filter, deserializer, allowExistTopic, user, password);
        } else {//主题不存在
           log.error("The configuration for the subscription topic does not exist.");
        }
    }

    /**
     * 取消订阅
     * @param topic
     */
    @Override
    public void unsubscribe(String topic) {
        try {
            String host = dolphindbConfig.getHost();
            Integer port = dolphindbConfig.getPort();
            String tableName = DolphindbUtils.getTopicTableName(topic);
            String actionName = tableName;//先使用表名称 TODO
            String key = topic + "_" + actionName;
            ThreadedClient threadedClient = DolphindbSubscribeClient.getThreadedClient(key);
            if (threadedClient != null){
                threadedClient.unsubscribe(host,port,tableName,actionName);
                threadedClient.close();
                DolphindbSubscribeClient.threadedClientMap.remove(key);
            }
        } catch (Exception e) {
            log.error("topic {} unsubscribe Exception:{}",topic,e.getMessage());
        }
    }

    @Override
    public void registerEventListener(EventListener listener) {
        Preconditions.checkNotNull(listener);
        dolphindbMessageHandler = new DolphindbMessageHandler(listener);
    }

}
