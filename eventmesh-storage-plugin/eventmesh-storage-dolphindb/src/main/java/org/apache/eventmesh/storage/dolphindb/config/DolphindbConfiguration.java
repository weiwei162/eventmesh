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

package org.apache.eventmesh.storage.dolphindb.config;

import lombok.Data;
import org.apache.eventmesh.common.config.Config;
import org.apache.eventmesh.common.config.ConfigFiled;

@Data
@Config(prefix = "eventMesh.server.dolphindb", path = "classPath://dolphindb-client.properties")
public class DolphindbConfiguration {


    /**
     * 是发布端节点的 IP 地址。
     */
    @ConfigFiled(field = "host")
    private String host;

    /**
     *  是一个整数 发布端节点的端口号
     */
    @ConfigFiled(field = "port")
    private Integer port;
    /**
     * 连接服务器的登录用户名
     */
    @ConfigFiled(field = "user")
    private String user;

    /**
     * 连接服务器的登录密码
     */
    @ConfigFiled(field = "password")
    private String password;

    /**
     * 是整数，表示订阅任务开始后的第一条消息所在的位置。消息是流数据表中的行。如果没有指定 offset，或它为负数或超过了流数据表的记录行数，订阅将会从流数据表的当前行开始。offset 与流数据表创建时的第一行对应。如果某些行因为内存限制被删除，在决定订阅开始的位置时，这些行仍然考虑在内。
     */
    @ConfigFiled(field = "offset")
    private Long offset;

    /**
     * 是布尔值，表示订阅中断后，是否会自动重订阅
     */
    @ConfigFiled(field = "reconnect")
    private Boolean reconnect = true;

    /**
     * 是一个整数，表示批处理的消息的数量。如果它是正数，直到消息的数量达到 batchSize 时，handler 才会处理进来的消息。如果它没有指定或者是非正数，消息到达之后，handler 就会马上处理消息。
     */
    @ConfigFiled(field = "batchSize")
    private Integer batchSize;

    /**
     * 是一个浮点数，表示 handler 处理到达的消息之前等待的时间，以秒为单位。默认值为 1。如果没有指定 batchSize，throttle 将不会起作用
     */
    @ConfigFiled(field = "throttle")
    private Integer throttle;

    @ConfigFiled(field = "loadBalance")
    private Boolean loadBalance = false;

    @ConfigFiled(field = "enableHighAvailability")
    private Boolean enableHighAvailability = false;

    @ConfigFiled(field = "connectionPoolCount")
    private Integer connectionPoolCount;

    @ConfigFiled(field = "subscribePoolCount")
    private Integer subscribePoolCount;

    @ConfigFiled(field = "topicTableName")
    private String topicTableName;
}
