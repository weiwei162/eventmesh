package org.apache.eventmesh.storage.dolphindb.streaming;

import com.xxdb.data.Entity;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.api.EventListener;
import org.apache.eventmesh.api.EventMeshAction;
import org.apache.eventmesh.api.EventMeshAsyncConsumeContext;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.storage.dolphindb.cloudevent.DolphindbCloudEvent;

@Slf4j
public class DolphindbMessageHandler implements MessageHandler {

    private final EventListener listener;
    public DolphindbMessageHandler(EventListener listener){
        this.listener = listener;
    }

    @Override
    public void doEvent(IMessage massage) {

        try {

            //收到消息数据
            Entity entity = massage.getEntity(1);
            String jsonMassage = entity.toString();
            //转换成CloudEvent消息对象
            DolphindbCloudEvent dolphindbCloudEvent = JsonUtils.parseObject(jsonMassage.getBytes(),DolphindbCloudEvent.class);
            CloudEvent cloudEvent = dolphindbCloudEvent.convertToCloudEvent();
            //
            final EventMeshAsyncConsumeContext consumeContext = new EventMeshAsyncConsumeContext() {

                @Override
                public void commit(EventMeshAction action) {
                    log.info("consumer event: {} finish action: {}", massage, action);
                }
            };
            //响应listener
            listener.consume(cloudEvent, consumeContext);
        } catch (Exception e) {//某条消息失败继续，不响应后续消息发送
            log.error("DolphindbMessageHandler doEvent Exception:{}",e.getMessage());
        }
    }
}
