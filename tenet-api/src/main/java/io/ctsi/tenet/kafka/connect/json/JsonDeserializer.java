package io.ctsi.tenet.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer implements Deserializer<JsonNode> {
    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonDeserializer() {
    }


    @Override
    public JsonNode deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        JsonNode data;
        try {
            data = objectMapper.readTree(bytes);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }
}
