package io.ctsi.tenet.kafka.connect.data;

import io.ctsi.tenet.kafka.connect.data.error.DataException;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Decimal {
        public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.Decimal";
        public static final String SCALE_FIELD = "scale";

        /**
         * Returns a SchemaBuilder for a Decimal with the given scale factor. By returning a SchemaBuilder you can override
         * additional schema settings such as required/optional, default value, and documentation.
         * @param scale the scale factor to apply to unscaled values
         * @return a SchemaBuilder
         */
        public static SchemaBuilder builder(int scale) {
            return SchemaBuilder.bytes()
                    .name(LOGICAL_NAME)
                    .parameter(SCALE_FIELD, Integer.toString(scale))
                    .version(1);
        }

        public static io.ctsi.tenet.kafka.connect.data.Schema schema(int scale) {
            return builder(scale).build();
        }

        /**
         * Convert a value from its logical format (BigDecimal) to it's encoded format.
         * @param value the logical value
         * @return the encoded value
         */
        public static byte[] fromLogical(io.ctsi.tenet.kafka.connect.data.Schema schema, BigDecimal value) {
            if (value.scale() != scale(schema))
                throw new DataException("BigDecimal has mismatching scale value for given Decimal schema");
            return value.unscaledValue().toByteArray();
        }

        public static BigDecimal toLogical(io.ctsi.tenet.kafka.connect.data.Schema schema, byte[] value) {
            return new BigDecimal(new BigInteger(value), scale(schema));
        }

        private static int scale(io.ctsi.tenet.kafka.connect.data.Schema schema) {
            String scaleString = schema.parameters().get(SCALE_FIELD);
            if (scaleString == null)
                throw new DataException("Invalid Decimal schema: scale parameter not found.");
            try {
                return Integer.parseInt(scaleString);
            } catch (NumberFormatException e) {
                throw new DataException("Invalid scale parameter found in Decimal schema: ", e);
            }
        }
}

