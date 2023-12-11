package org.apache.eventmesh.storage.dolphindb.consumer;

import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.api.AbstractContext;
import org.apache.eventmesh.api.EventListener;
import org.apache.eventmesh.api.consumer.Consumer;

import java.util.List;
import java.util.Properties;

/**
 * DolphindbConsumer类Adaptor
 */
@Slf4j
public class DolphindbConsumerAdaptor implements Consumer {

    private DolphindbConsumer consumer;

    @Override
    public boolean isStarted() {
        return consumer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return consumer.isClosed();
    }

    @Override
    public void start() {
        consumer.start();
    }

    @Override
    public void shutdown() {
        consumer.shutdown();
    }

    @Override
    public void init(Properties keyValue) throws Exception {
        consumer = new DolphindbConsumer(keyValue);
        consumer.init(keyValue);
    }

    @Override
    public void updateOffset(List<CloudEvent> cloudEvents, AbstractContext context) {
        consumer.updateOffset(cloudEvents,context);
    }

    @Override
    public void subscribe(String topic) throws Exception {
        consumer.subscribe(topic);
    }

    @Override
    public void unsubscribe(String topic) {
        consumer.unsubscribe(topic);
    }

    @Override
    public void registerEventListener(EventListener listener) {
        consumer.registerEventListener(listener);
    }

}
