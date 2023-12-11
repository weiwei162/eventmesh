package org.apache.eventmesh.storage.dolphindb.cloudevent;

import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.message.impl.BaseGenericBinaryMessageReaderImpl;

import java.util.function.BiConsumer;

public class DolphindbCloudEventReader extends BaseGenericBinaryMessageReaderImpl<String, String> {


    protected DolphindbCloudEventReader(SpecVersion version, CloudEventData body) {
        super(version, body);
    }

    @Override
    protected boolean isContentTypeHeader(String key) {
        return false;
    }

    @Override
    protected boolean isCloudEventsHeader(String key) {
        return false;
    }

    @Override
    protected String toCloudEventsKey(String key) {
        return null;
    }

    @Override
    protected void forEachHeader(BiConsumer<String, String> fn) {

    }

    @Override
    protected String toCloudEventsValue(String value) {
        return null;
    }
}
