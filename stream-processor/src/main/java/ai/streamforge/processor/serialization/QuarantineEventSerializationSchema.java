package ai.streamforge.processor.serialization;

import ai.streamforge.processor.model.QuarantineEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Serializes {@link QuarantineEvent} to JSON bytes for the quarantine Kafka topic. */
public class QuarantineEventSerializationSchema implements SerializationSchema<QuarantineEvent> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(QuarantineEvent element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize QuarantineEvent: " + element, e);
        }
    }
}
