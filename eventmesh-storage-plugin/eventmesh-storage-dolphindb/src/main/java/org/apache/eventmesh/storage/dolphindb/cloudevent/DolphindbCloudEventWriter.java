package org.apache.eventmesh.storage.dolphindb.cloudevent;

import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.message.MessageWriter;
import io.cloudevents.rw.CloudEventContextWriter;
import io.cloudevents.rw.CloudEventRWException;
import io.cloudevents.rw.CloudEventWriter;

import java.nio.charset.StandardCharsets;

/**
 * 用于读取 CloudEvent消息数据写入到 DolphindbCloudEvent
 */
public class DolphindbCloudEventWriter
       implements MessageWriter<CloudEventWriter<DolphindbCloudEvent>, DolphindbCloudEvent>,
                  CloudEventWriter<DolphindbCloudEvent> {

        private final DolphindbCloudEvent dolphindbCloudEvent;

        public DolphindbCloudEventWriter (){
            dolphindbCloudEvent = new DolphindbCloudEvent();
        }
        @Override
        public DolphindbCloudEvent setEvent(EventFormat format, byte[] value) throws CloudEventRWException {
            dolphindbCloudEvent.setData(new String(value, StandardCharsets.UTF_8));
            return dolphindbCloudEvent;
        }

        @Override
        public DolphindbCloudEvent end(CloudEventData data) throws CloudEventRWException {
            dolphindbCloudEvent.setData(new String(data.toBytes(), StandardCharsets.UTF_8));
            return dolphindbCloudEvent;
        }

        @Override
        public DolphindbCloudEvent end() throws CloudEventRWException {
            dolphindbCloudEvent.setData("");
            return dolphindbCloudEvent;
        }

        @Override
        public CloudEventContextWriter withContextAttribute(String name, String value) throws CloudEventRWException {
            dolphindbCloudEvent.getExtensions().put(name,value);
            return this;
        }

        @Override
        public CloudEventWriter<DolphindbCloudEvent> create(SpecVersion version) throws CloudEventRWException {
            dolphindbCloudEvent.setVersion(version);
            return this;
        }

}
