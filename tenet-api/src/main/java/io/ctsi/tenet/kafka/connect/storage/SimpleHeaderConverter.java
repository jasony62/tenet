package io.ctsi.tenet.kafka.connect.storage;

import io.ctsi.tenet.kafka.connect.data.HeaderConverter;
import io.ctsi.tenet.kafka.connect.data.Schema;
import io.ctsi.tenet.kafka.connect.data.SchemaAndValue;
import io.ctsi.tenet.kafka.connect.data.Values;
import io.ctsi.tenet.kafka.connect.data.error.DataException;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;

public class SimpleHeaderConverter implements HeaderConverter {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleHeaderConverter.class);
    private static final ConfigDef CONFIG_DEF = new ConfigDef();
    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(null, null);
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // do nothing
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        if (value == null) {
            return NULL_SCHEMA_AND_VALUE;
        }
        try {
            String str = new String(value, UTF_8);
            if (str.isEmpty()) {
                return new SchemaAndValue(Schema.STRING_SCHEMA, str);
            }
            return Values.parseString(str);
        } catch (NoSuchElementException e) {
            throw new DataException("Failed to deserialize value for header '" + headerKey + "' on topic '" + topic + "'", e);
        } catch (Throwable t) {
            LOG.warn("Failed to deserialize value for header '{}' on topic '{}', so using byte array", headerKey, topic, t);
            return new SchemaAndValue(Schema.BYTES_SCHEMA, value);
        }
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        return Values.convertToString(schema, value).getBytes(UTF_8);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}

