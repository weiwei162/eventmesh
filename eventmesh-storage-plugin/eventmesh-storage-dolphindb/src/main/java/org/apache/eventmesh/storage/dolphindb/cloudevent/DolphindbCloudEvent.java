package org.apache.eventmesh.storage.dolphindb.cloudevent;

import com.fasterxml.jackson.core.type.TypeReference;
import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.Constants;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.storage.dolphindb.utils.ByteArrayUtils;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *  Dolphindb 的 CloudEvent 消息对象
 */
@Slf4j
@Data
@NoArgsConstructor
public class DolphindbCloudEvent implements Serializable {

    private SpecVersion version;
    private String data;
    private Map<String, String> extensions = new HashMap<>();

    public CloudEvent convertToCloudEvent()  {
        CloudEventBuilder builder;
        switch (version) {
            case V03:
                builder = CloudEventBuilder.v03();
                break;
            case V1:
                builder = CloudEventBuilder.v1();
                break;
            default:
                builder = CloudEventBuilder.v1();
        }
        builder.withData(data.getBytes(StandardCharsets.UTF_8))
                .withId(extensions.remove("id"))
                .withSource(URI.create(extensions.remove("source")))
                .withType(extensions.remove("type"))
                .withDataContentType(extensions.remove("datacontenttype"))
                .withSubject(extensions.remove("subject"));
        extensions.forEach(builder::withExtension);

        return builder.build();
    }

    public static byte[] toByteArray(DolphindbCloudEvent rabbitmqCloudEvent) throws Exception {
        Optional<byte[]> optionalBytes = ByteArrayUtils.objectToBytes(rabbitmqCloudEvent);
        return optionalBytes.orElseGet(() -> new byte[]{});
    }

    public static DolphindbCloudEvent getFromByteArray(byte[] body) {
        return JsonUtils.parseTypeReferenceObject(new String(body, Constants.DEFAULT_CHARSET), new TypeReference<DolphindbCloudEvent>() {
        });
    }
}
