package org.apache.eventmesh.storage.dolphindb.producer;

import io.cloudevents.CloudEvent;
import org.apache.eventmesh.api.RequestReplyCallback;
import org.apache.eventmesh.api.SendCallback;
import org.apache.eventmesh.api.producer.Producer;

import java.util.Properties;

public class DolphindbProducerAdaptor implements Producer {

    private DolphindbProducer producer;

    @Override
    public boolean isStarted() {
        return producer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return producer.isClosed();
    }

    @Override
    public void start() {
        producer.start();
    }

    @Override
    public void shutdown() {
        producer.shutdown();
    }

    @Override
    public void init(Properties properties) throws Exception {
        producer = new DolphindbProducer(properties);
        producer.init(properties);
    }

    @Override
    public void publish(CloudEvent cloudEvent, SendCallback sendCallback) throws Exception {
        producer.publish(cloudEvent,sendCallback);
    }

    @Override
    public void sendOneway(CloudEvent cloudEvent) {
        producer.sendOneway(cloudEvent);
    }

    @Override
    public void request(CloudEvent cloudEvent, RequestReplyCallback rrCallback, long timeout) throws Exception {
        producer.request(cloudEvent,rrCallback,timeout);
    }

    @Override
    public boolean reply(CloudEvent cloudEvent, SendCallback sendCallback) throws Exception {
        return producer.reply(cloudEvent,sendCallback);
    }

    @Override
    public void checkTopicExist(String topic) throws Exception {
        producer.checkTopicExist(topic);
    }

    @Override
    public void setExtFields() {
        producer.setExtFields();
    }
}
