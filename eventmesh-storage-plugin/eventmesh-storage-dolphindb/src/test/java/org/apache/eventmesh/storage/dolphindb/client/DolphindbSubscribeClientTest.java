package org.apache.eventmesh.storage.dolphindb.client;

import com.xxdb.data.Entity;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.*;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.storage.dolphindb.config.DolphindbConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

class DolphindbSubscribeClientTest {

    @Test
    void testSubscribe() throws IOException {
        // Setup
        // Run the test
        DolphindbConfiguration dolphindbConfig;
        ConfigService configService = ConfigService.getInstance();
        dolphindbConfig = configService.buildConfigInstance(DolphindbConfiguration.class);
        //ThreadPooledClient subscribePoolClient;
        //subscribePoolClient = DolphindbSubscribeClient.INSTANCE;

        String host = dolphindbConfig.getHost();
        Integer port = dolphindbConfig.getPort();
        String tableName = "t_test01";
        String actionName = "test1";
        MessageHandler handler = null;
        Long offset = dolphindbConfig.getOffset();
        //Boolean reconnect = dolphindbConfig.getReconnect();
        Boolean reconnect = false;
        Vector filter = null;
        //Integer batchSize = dolphindbConfig.getBatchSize();
        //float throttle = 1;
        StreamDeserializer deserializer = null;
        Boolean allowExistTopic = true;
        String user = dolphindbConfig.getUser();
        String password = dolphindbConfig.getPassword();

        //subscribePoolClient.subscribe(host,port,tableName,actionName,new DolphindbMessageHandler(),offset,reconnect,filter,deserializer,allowExistTopic,user,password);

//        ThreadedClient client = new ThreadedClient();
//        client.subscribe(host,port,tableName,actionName,new DolphindbMessageHandler(),0,reconnect,filter,deserializer,allowExistTopic,user,password);

        PollingClient client = new PollingClient();
        //TopicPoller poller1 = client.subscribe(host, port, tableName,actionName, 1,reconnect,filter,deserializer,user,password);
        TopicPoller poller1 = client.subscribe(host, port, tableName,actionName, -1,true);

        while (true) {
            ArrayList<IMessage> msgs = poller1.poll(1000);
            if (msgs.size() > 0) {
                System.out.println(msgs);
                //Entity entity = msgs.get(0).getEntity(2);  //取数据中第一行第三个字段
                IMessage message = msgs.get(0);
                System.out.println("========"+message.toString());
                System.out.println(message.getEntity(1));
                Entity entity = message.getEntity(1);
                String jsonMassage = entity.toString();
//                DolphindbCloudEvent rabbitmqCloudEvent = DolphindbCloudEvent.getFromByteArray(jsonMassage.getBytes());
//                rabbitmqCloudEvent.setVersion(SpecVersion.V1);
//                rabbitmqCloudEvent.setData("");
//                CloudEvent cloudEvent = rabbitmqCloudEvent.convertToCloudEvent();
                EventFormat eventFormat = Objects.requireNonNull(
                        EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE));

                CloudEvent cloudEvent = eventFormat.deserialize(jsonMassage.getBytes());
                System.out.println(cloudEvent);
            }
        }
        // Verify the results
    }
}
