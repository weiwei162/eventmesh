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

package org.apache.eventmesh.storage.dolphindb.producer;

import com.google.common.base.Preconditions;
import com.xxdb.ExclusiveDBConnectionPool;
import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.api.RequestReplyCallback;
import org.apache.eventmesh.api.SendCallback;
import org.apache.eventmesh.api.SendResult;
import org.apache.eventmesh.api.exception.OnExceptionContext;
import org.apache.eventmesh.api.exception.StorageRuntimeException;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.storage.dolphindb.client.DolphindbConnectionClient;
import org.apache.eventmesh.storage.dolphindb.cloudevent.DolphindbCloudEvent;
import org.apache.eventmesh.storage.dolphindb.cloudevent.DolphindbCloudEventWriter;
import org.apache.eventmesh.storage.dolphindb.utils.DolphindbUtils;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 生成者
 */
@Slf4j
public class DolphindbProducer {

    private AtomicBoolean isStarted;

    //private ThreadPooledClient subscribePoolClient;

    private ExclusiveDBConnectionPool connectionPool;

    public DolphindbProducer(Properties properties) {
        this.isStarted = new AtomicBoolean(false);
    }

    public boolean isStarted() {
        return isStarted.get();
    }

    public boolean isClosed() {
        return !isStarted.get();
    }

    public void start() {
        isStarted.compareAndSet(false, true);
    }

    public void shutdown() {
        isStarted.compareAndSet(true, false);
    }

    public void init(Properties properties) throws Exception {
        //subscribePoolClient = DolphindbSubscribeClient.INSTANCE;
        connectionPool = DolphindbConnectionClient.INSTANCE;
    }

    public void publish(CloudEvent cloudEvent, SendCallback sendCallback)  {
        Preconditions.checkNotNull(cloudEvent);
        Preconditions.checkNotNull(sendCallback);

        String topic = cloudEvent.getSubject();
        //String topicTable = DolphindbUtils.topicAndTableMap.get(topic);
        //数据流表表名称
        String topicTable = DolphindbUtils.getTopicTableName(topic);//获取对应的Dolphindb数据流表
        if (topicTable == null) {//检查主题是否存在存放消息的表皮配置
            //失败回复
            sendCallback.onException(
                    OnExceptionContext.builder()
                            .topic(cloudEvent.getSubject())
                            .messageId(cloudEvent.getId())
                            .exception(new StorageRuntimeException("The topic does not exist."))
                            .build());
        }
        //构建DolphindbCloudEvent消息对象
        DolphindbCloudEventWriter dolphindbCloudEventWriter = new DolphindbCloudEventWriter();
        DolphindbCloudEvent dolphindbCloudEvent = dolphindbCloudEventWriter.writeBinary(cloudEvent);
        String jsonMessage = JsonUtils.toJSONString(dolphindbCloudEvent);
        //处理json存转义符号入数据库后转义符号丢失的问题
        jsonMessage = jsonMessage.replace("\\\"","\\\\\"");
        //String jsonMessage = new String(Objects.requireNonNull(cloudEvent.getData()).toBytes(), Constants.DEFAULT_CHARSET);
        //消息写入Dolphindb数据库
        String errorMsg = DolphindbUtils.publish(topicTable, topic, jsonMessage);
        if (errorMsg != null) {//消息写入Dolphindb数据出错
            //失败回复
            sendCallback.onException(
                    OnExceptionContext.builder()
                            .topic(cloudEvent.getSubject())
                            .messageId(cloudEvent.getId())
                            .exception(new StorageRuntimeException(errorMsg))
                            .build());
        } else {
            //成功回复
            SendResult sendResult = new SendResult();
            sendResult.setTopic(cloudEvent.getSubject());
            sendResult.setMessageId(cloudEvent.getId());
            sendCallback.onSuccess(sendResult);
        }

    }

    public void sendOneway(CloudEvent cloudEvent) {
        String topic = cloudEvent.getSubject();
        //数据流表表名称
        String topicTable = DolphindbUtils.topicAndTableMap.get(topic);
        if (topicTable != null) {
            //构建DolphindbCloudEvent消息对象
            DolphindbCloudEventWriter dolphindbCloudEventWriter = new DolphindbCloudEventWriter();
            DolphindbCloudEvent dolphindbCloudEvent = dolphindbCloudEventWriter.writeBinary(cloudEvent);
            String jsonMessage = JsonUtils.toJSONString(dolphindbCloudEvent);
            //String jsonMessage = new String(Objects.requireNonNull(cloudEvent.getData()).toBytes(), Constants.DEFAULT_CHARSET);
            //处理json存转义符号入数据库后转义符号丢失的问题
            jsonMessage = jsonMessage.replace("\\\"","\\\\\"");
            String errorMsg = DolphindbUtils.publish(topicTable, topic, jsonMessage);
            if (errorMsg != null){
                log.error("topic:{} sendOneway errorMsg:{}",topic,errorMsg);
            }
        } else {
            log.error("The topic does not exist.");
        }
    }

    public void request(CloudEvent cloudEvent, RequestReplyCallback rrCallback, long timeout) throws Exception {
        throw new StorageRuntimeException("Request is not supported");
    }

    public boolean reply(CloudEvent cloudEvent, SendCallback sendCallback) throws Exception {
        throw new StorageRuntimeException("Reply is not supported");
    }

    public void checkTopicExist(String topic) throws Exception {
        //getStreamingStat().pubTables 检查topic是否存在 TODO
    }

    public void setExtFields() {

    }

}
