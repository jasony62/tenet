package io.ctsi.tenet.kafka.connect.data;

import io.ctsi.tenet.kafka.connect.data.error.DataException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.*;
import java.util.*;
import java.util.regex.Pattern;

public class Values {

    private static final Logger LOG = LoggerFactory.getLogger(Values.class);

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final io.ctsi.tenet.kafka.connect.data.SchemaAndValue NULL_SCHEMA_AND_VALUE = new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(null, null);
    private static final io.ctsi.tenet.kafka.connect.data.SchemaAndValue TRUE_SCHEMA_AND_VALUE = new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.BOOLEAN_SCHEMA, Boolean.TRUE);
    private static final io.ctsi.tenet.kafka.connect.data.SchemaAndValue FALSE_SCHEMA_AND_VALUE = new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.BOOLEAN_SCHEMA, Boolean.FALSE);
    private static final io.ctsi.tenet.kafka.connect.data.Schema ARRAY_SELECTOR_SCHEMA = SchemaBuilder.array(io.ctsi.tenet.kafka.connect.data.Schema.STRING_SCHEMA).build();
    private static final io.ctsi.tenet.kafka.connect.data.Schema MAP_SELECTOR_SCHEMA = SchemaBuilder.map(io.ctsi.tenet.kafka.connect.data.Schema.STRING_SCHEMA, io.ctsi.tenet.kafka.connect.data.Schema.STRING_SCHEMA).build();
    private static final io.ctsi.tenet.kafka.connect.data.Schema STRUCT_SELECTOR_SCHEMA = SchemaBuilder.struct().build();
    private static final String TRUE_LITERAL = Boolean.TRUE.toString();
    private static final String FALSE_LITERAL = Boolean.FALSE.toString();
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final String NULL_VALUE = "null";
    static final String ISO_8601_DATE_FORMAT_PATTERN = "yyyy-MM-dd";
    static final String ISO_8601_TIME_FORMAT_PATTERN = "HH:mm:ss.SSS'Z'";
    static final String ISO_8601_TIMESTAMP_FORMAT_PATTERN = ISO_8601_DATE_FORMAT_PATTERN + "'T'" + ISO_8601_TIME_FORMAT_PATTERN;
    private static final Set<String> TEMPORAL_LOGICAL_TYPE_NAMES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(io.ctsi.tenet.kafka.connect.data.Time.LOGICAL_NAME,
                                    io.ctsi.tenet.kafka.connect.data.Timestamp.LOGICAL_NAME,
                                    Date.LOGICAL_NAME
                            )
                    )
            );

    private static final String QUOTE_DELIMITER = "\"";
    private static final String COMMA_DELIMITER = ",";
    private static final String ENTRY_DELIMITER = ":";
    private static final String ARRAY_BEGIN_DELIMITER = "[";
    private static final String ARRAY_END_DELIMITER = "]";
    private static final String MAP_BEGIN_DELIMITER = "{";
    private static final String MAP_END_DELIMITER = "}";
    private static final int ISO_8601_DATE_LENGTH = ISO_8601_DATE_FORMAT_PATTERN.length();
    private static final int ISO_8601_TIME_LENGTH = ISO_8601_TIME_FORMAT_PATTERN.length() - 2; // subtract single quotes
    private static final int ISO_8601_TIMESTAMP_LENGTH = ISO_8601_TIMESTAMP_FORMAT_PATTERN.length() - 4; // subtract single quotes

    private static final Pattern TWO_BACKSLASHES = Pattern.compile("\\\\");

    private static final Pattern DOUBLEQOUTE = Pattern.compile("\"");

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#BOOLEAN} value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a boolean.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a boolean, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a boolean
     */
    public static Boolean convertToBoolean(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) throws DataException {
        return (Boolean) convertTo(io.ctsi.tenet.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#INT8} byte value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a byte.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a byte, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a byte
     */
    public static Byte convertToByte(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) throws DataException {
        return (Byte) convertTo(io.ctsi.tenet.kafka.connect.data.Schema.OPTIONAL_INT8_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#INT16} short value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a short.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a short, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a short
     */
    public static Short convertToShort(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) throws DataException {
        return (Short) convertTo(io.ctsi.tenet.kafka.connect.data.Schema.OPTIONAL_INT16_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#INT32} int value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to an integer.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as an integer, or null if the supplied value was null
     * @throws DataException if the value could not be converted to an integer
     */
    public static Integer convertToInteger(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) throws DataException {
        return (Integer) convertTo(io.ctsi.tenet.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#INT64} long value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a long.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a long, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a long
     */
    public static Long convertToLong(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) throws DataException {
        return (Long) convertTo(io.ctsi.tenet.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#FLOAT32} float value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a floating point number.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a float, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a float
     */
    public static Float convertToFloat(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) throws DataException {
        return (Float) convertTo(io.ctsi.tenet.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#FLOAT64} double value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a floating point number.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a double, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a double
     */
    public static Double convertToDouble(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) throws DataException {
        return (Double) convertTo(io.ctsi.tenet.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#STRING} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a string, or null if the supplied value was null
     */
    public static String convertToString(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) {
        return (String) convertTo(io.ctsi.tenet.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#ARRAY} value. If the value is a string representation of an array, this method
     * will parse the string and its elements to infer the schemas for those elements. Thus, this method supports
     * arrays of other primitives and structured types. If the value is already an array (or list), this method simply casts and
     * returns it.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a list, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a list value
     */
    public static List<?> convertToList(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) {
        return (List<?>) convertTo(ARRAY_SELECTOR_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#MAP} value. If the value is a string representation of a map, this method
     * will parse the string and its entries to infer the schemas for those entries. Thus, this method supports
     * maps with primitives and structured keys and values. If the value is already a map, this method simply casts and returns it.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a map, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a map value
     */
    public static Map<?, ?> convertToMap(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) {
        return (Map<?, ?>) convertTo(MAP_SELECTOR_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Schema.Type#STRUCT} value. Structs cannot be converted from other types, so this method returns
     * a struct only if the supplied value is a struct. If not a struct, this method throws an exception.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a struct, or null if the supplied value was null
     * @throws DataException if the value is not a struct
     */
    public static io.ctsi.tenet.kafka.connect.data.Struct convertToStruct(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) {
        return (io.ctsi.tenet.kafka.connect.data.Struct) convertTo(STRUCT_SELECTOR_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Time#SCHEMA time} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a time, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a time value
     */
    public static Date convertToTime(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) {
        return (Date) convertTo(io.ctsi.tenet.kafka.connect.data.Time.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Date#SCHEMA date} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a date, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a date value
     */
    public static Date convertToDate(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) {
        return (Date) convertTo(Date.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Timestamp#SCHEMA timestamp} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a timestamp, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a timestamp value
     */
    public static Date convertToTimestamp(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) {
        return (Date) convertTo(io.ctsi.tenet.kafka.connect.data.Timestamp.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link io.ctsi.tenet.kafka.connect.data.Decimal decimal} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a decimal, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a decimal value
     */
    public static BigDecimal convertToDecimal(io.ctsi.tenet.kafka.connect.data.Schema schema, Object value, int scale) {
        return (BigDecimal) convertTo(io.ctsi.tenet.kafka.connect.data.Decimal.schema(scale), schema, value);
    }

    /**
     * If possible infer a schema for the given value.
     *
     * @param value the value whose schema is to be inferred; may be null
     * @return the inferred schema, or null if the value is null or no schema could be inferred
     */
    public static io.ctsi.tenet.kafka.connect.data.Schema inferSchema(Object value) {
        if (value instanceof String) {
            return io.ctsi.tenet.kafka.connect.data.Schema.STRING_SCHEMA;
        }
        if (value instanceof Boolean) {
            return io.ctsi.tenet.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
        }
        if (value instanceof Byte) {
            return io.ctsi.tenet.kafka.connect.data.Schema.INT8_SCHEMA;
        }
        if (value instanceof Short) {
            return io.ctsi.tenet.kafka.connect.data.Schema.INT16_SCHEMA;
        }
        if (value instanceof Integer) {
            return io.ctsi.tenet.kafka.connect.data.Schema.INT32_SCHEMA;
        }
        if (value instanceof Long) {
            return io.ctsi.tenet.kafka.connect.data.Schema.INT64_SCHEMA;
        }
        if (value instanceof Float) {
            return io.ctsi.tenet.kafka.connect.data.Schema.FLOAT32_SCHEMA;
        }
        if (value instanceof Double) {
            return io.ctsi.tenet.kafka.connect.data.Schema.FLOAT64_SCHEMA;
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            return io.ctsi.tenet.kafka.connect.data.Schema.BYTES_SCHEMA;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                return null;
            }
            SchemaDetector detector = new SchemaDetector();
            for (Object element : list) {
                if (!detector.canDetect(element)) {
                    return null;
                }
            }
            return SchemaBuilder.array(detector.schema()).build();
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            if (map.isEmpty()) {
                return null;
            }
            SchemaDetector keyDetector = new SchemaDetector();
            SchemaDetector valueDetector = new SchemaDetector();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!keyDetector.canDetect(entry.getKey()) || !valueDetector.canDetect(entry.getValue())) {
                    return null;
                }
            }
            return SchemaBuilder.map(keyDetector.schema(), valueDetector.schema()).build();
        }
        if (value instanceof io.ctsi.tenet.kafka.connect.data.Struct) {
            return ((io.ctsi.tenet.kafka.connect.data.Struct) value).schema();
        }
        return null;
    }


    /**
     * Parse the specified string representation of a value into its schema and value.
     *
     * @param value the string form of the value
     * @return the schema and value; never null, but whose schema and value may be null
     * @see #convertToString
     */
    public static io.ctsi.tenet.kafka.connect.data.SchemaAndValue parseString(String value) {
        if (value == null) {
            return NULL_SCHEMA_AND_VALUE;
        }
        if (value.isEmpty()) {
            return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.STRING_SCHEMA, value);
        }
        Parser parser = new Parser(value);
        return parse(parser, false);
    }

    /**
     * Convert the value to the desired type.
     *
     * @param toSchema   the schema for the desired type; may not be null
     * @param fromSchema the schema for the supplied value; may be null if not known
     * @return the converted value; never null
     * @throws DataException if the value could not be converted to the desired type
     */
    protected static Object convertTo(io.ctsi.tenet.kafka.connect.data.Schema toSchema, io.ctsi.tenet.kafka.connect.data.Schema fromSchema, Object value) throws DataException {
        if (value == null) {
            if (toSchema.isOptional()) {
                return null;
            }
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        switch (toSchema.type()) {
            case BYTES:
                if (io.ctsi.tenet.kafka.connect.data.Decimal.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof ByteBuffer) {
                        value = Utils.toArray((ByteBuffer) value);
                    }
                    if (value instanceof byte[]) {
                        return io.ctsi.tenet.kafka.connect.data.Decimal.toLogical(toSchema, (byte[]) value);
                    }
                    if (value instanceof BigDecimal) {
                        return value;
                    }
                    if (value instanceof Number) {
                        // Not already a decimal, so treat it as a double ...
                        double converted = ((Number) value).doubleValue();
                        return BigDecimal.valueOf(converted);
                    }
                    if (value instanceof String) {
                        return new BigDecimal(value.toString()).doubleValue();
                    }
                }
                if (value instanceof ByteBuffer) {
                    return Utils.toArray((ByteBuffer) value);
                }
                if (value instanceof byte[]) {
                    return value;
                }
                if (value instanceof BigDecimal) {
                    return io.ctsi.tenet.kafka.connect.data.Decimal.fromLogical(toSchema, (BigDecimal) value);
                }
                break;
            case STRING:
                StringBuilder sb = new StringBuilder();
                append(sb, value, false);
                return sb.toString();
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof String) {
                    io.ctsi.tenet.kafka.connect.data.SchemaAndValue parsed = parseString(value.toString());
                    if (parsed.value() instanceof Boolean) {
                        return parsed.value();
                    }
                }
                return asLong(value, fromSchema, null) == 0L ? Boolean.FALSE : Boolean.TRUE;
            case INT8:
                if (value instanceof Byte) {
                    return value;
                }
                return (byte) asLong(value, fromSchema, null);
            case INT16:
                if (value instanceof Short) {
                    return value;
                }
                return (short) asLong(value, fromSchema, null);
            case INT32:
                if (Date.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        io.ctsi.tenet.kafka.connect.data.SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof Date) {
                        if (fromSchema != null) {
                            String fromSchemaName = fromSchema.name();
                            if (Date.LOGICAL_NAME.equals(fromSchemaName)) {
                                return value;
                            }
                            if (io.ctsi.tenet.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                                // Just get the number of days from this timestamp
                                long millis = ((Date) value).getTime();
                                int days = (int) (millis / MILLIS_PER_DAY); // truncates
                                return Date.toLogical(toSchema, days);
                            }
                        } else {
                            // There is no fromSchema, so no conversion is needed
                            return value;
                        }
                    }
                    long numeric = asLong(value, fromSchema, null);
                    return Date.toLogical(toSchema, (int) numeric);
                }
                if (io.ctsi.tenet.kafka.connect.data.Time.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        io.ctsi.tenet.kafka.connect.data.SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof Date) {
                        if (fromSchema != null) {
                            String fromSchemaName = fromSchema.name();
                            if (io.ctsi.tenet.kafka.connect.data.Time.LOGICAL_NAME.equals(fromSchemaName)) {
                                return value;
                            }
                            if (io.ctsi.tenet.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                                // Just get the time portion of this timestamp
                                Calendar calendar = Calendar.getInstance(UTC);
                                calendar.setTime((Date) value);
                                calendar.set(Calendar.YEAR, 1970);
                                calendar.set(Calendar.MONTH, 0); // Months are zero-based
                                calendar.set(Calendar.DAY_OF_MONTH, 1);
                                return io.ctsi.tenet.kafka.connect.data.Time.toLogical(toSchema, (int) calendar.getTimeInMillis());
                            }
                        } else {
                            // There is no fromSchema, so no conversion is needed
                            return value;
                        }
                    }
                    long numeric = asLong(value, fromSchema, null);
                    return io.ctsi.tenet.kafka.connect.data.Time.toLogical(toSchema, (int) numeric);
                }
                if (value instanceof Integer) {
                    return value;
                }
                return (int) asLong(value, fromSchema, null);
            case INT64:
                if (io.ctsi.tenet.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        io.ctsi.tenet.kafka.connect.data.SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof Date) {
                        Date date = (Date) value;
                        if (fromSchema != null) {
                            String fromSchemaName = fromSchema.name();
                            if (Date.LOGICAL_NAME.equals(fromSchemaName)) {
                                int days = Date.fromLogical(fromSchema, date);
                                long millis = days * MILLIS_PER_DAY;
                                return io.ctsi.tenet.kafka.connect.data.Timestamp.toLogical(toSchema, millis);
                            }
                            if (io.ctsi.tenet.kafka.connect.data.Time.LOGICAL_NAME.equals(fromSchemaName)) {
                                long millis = io.ctsi.tenet.kafka.connect.data.Time.fromLogical(fromSchema, date);
                                return io.ctsi.tenet.kafka.connect.data.Timestamp.toLogical(toSchema, millis);
                            }
                            if (io.ctsi.tenet.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                                return value;
                            }
                        } else {
                            // There is no fromSchema, so no conversion is needed
                            return value;
                        }
                    }
                    long numeric = asLong(value, fromSchema, null);
                    return io.ctsi.tenet.kafka.connect.data.Timestamp.toLogical(toSchema, numeric);
                }
                if (value instanceof Long) {
                    return value;
                }
                return asLong(value, fromSchema, null);
            case FLOAT32:
                if (value instanceof Float) {
                    return value;
                }
                return (float) asDouble(value, fromSchema, null);
            case FLOAT64:
                if (value instanceof Double) {
                    return value;
                }
                return asDouble(value, fromSchema, null);
            case ARRAY:
                if (value instanceof String) {
                    io.ctsi.tenet.kafka.connect.data.SchemaAndValue schemaAndValue = parseString(value.toString());
                    value = schemaAndValue.value();
                }
                if (value instanceof List) {
                    return value;
                }
                break;
            case MAP:
                if (value instanceof String) {
                    io.ctsi.tenet.kafka.connect.data.SchemaAndValue schemaAndValue = parseString(value.toString());
                    value = schemaAndValue.value();
                }
                if (value instanceof Map) {
                    return value;
                }
                break;
            case STRUCT:
                if (value instanceof io.ctsi.tenet.kafka.connect.data.Struct) {
                    io.ctsi.tenet.kafka.connect.data.Struct struct = (io.ctsi.tenet.kafka.connect.data.Struct) value;
                    return struct;
                }
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    /**
     * Convert the specified value to the desired scalar value type.
     *
     * @param value      the value to be converted; may not be null
     * @param fromSchema the schema for the current value type; may not be null
     * @param error      any previous error that should be included in an exception message; may be null
     * @return the long value after conversion; never null
     * @throws DataException if the value could not be converted to a long
     */
    protected static long asLong(Object value, io.ctsi.tenet.kafka.connect.data.Schema fromSchema, Throwable error) {
        try {
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.longValue();
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString()).longValue();
            }
        } catch (NumberFormatException e) {
            error = e;
            // fall through
        }
        if (fromSchema != null) {
            String schemaName = fromSchema.name();
            if (value instanceof Date) {
                if (Date.LOGICAL_NAME.equals(schemaName)) {
                    return Date.fromLogical(fromSchema, (Date) value);
                }
                if (io.ctsi.tenet.kafka.connect.data.Time.LOGICAL_NAME.equals(schemaName)) {
                    return io.ctsi.tenet.kafka.connect.data.Time.fromLogical(fromSchema, (Date) value);
                }
                if (io.ctsi.tenet.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(schemaName)) {
                    return io.ctsi.tenet.kafka.connect.data.Timestamp.fromLogical(fromSchema, (Date) value);
                }
            }
            throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + fromSchema, error);
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to a number", error);
    }

    /**
     * Convert the specified value with the desired floating point type.
     *
     * @param value  the value to be converted; may not be null
     * @param schema the schema for the current value type; may not be null
     * @param error  any previous error that should be included in an exception message; may be null
     * @return the double value after conversion; never null
     * @throws DataException if the value could not be converted to a double
     */
    protected static double asDouble(Object value, io.ctsi.tenet.kafka.connect.data.Schema schema, Throwable error) {
        try {
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.doubleValue();
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString()).doubleValue();
            }
        } catch (NumberFormatException e) {
            error = e;
            // fall through
        }
        return asLong(value, schema, error);
    }

    protected static void append(StringBuilder sb, Object value, boolean embedded) {
        if (value == null) {
            sb.append(NULL_VALUE);
        } else if (value instanceof Number) {
            sb.append(value);
        } else if (value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof String) {
            if (embedded) {
                String escaped = escape((String) value);
                sb.append('"').append(escaped).append('"');
            } else {
                sb.append(value);
            }
        } else if (value instanceof byte[]) {
            value = Base64.getEncoder().encodeToString((byte[]) value);
            if (embedded) {
                sb.append('"').append(value).append('"');
            } else {
                sb.append(value);
            }
        } else if (value instanceof ByteBuffer) {
            byte[] bytes = Utils.readBytes((ByteBuffer) value);
            append(sb, bytes, embedded);
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            sb.append('[');
            appendIterable(sb, list.iterator());
            sb.append(']');
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            sb.append('{');
            appendIterable(sb, map.entrySet().iterator());
            sb.append('}');
        } else if (value instanceof io.ctsi.tenet.kafka.connect.data.Struct) {
            io.ctsi.tenet.kafka.connect.data.Struct struct = (io.ctsi.tenet.kafka.connect.data.Struct) value;
            io.ctsi.tenet.kafka.connect.data.Schema schema = struct.schema();
            boolean first = true;
            sb.append('{');
            for (Field field : schema.fields()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                append(sb, field.name(), true);
                sb.append(':');
                append(sb, struct.get(field), true);
            }
            sb.append('}');
        } else if (value instanceof Map.Entry) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) value;
            append(sb, entry.getKey(), true);
            sb.append(':');
            append(sb, entry.getValue(), true);
        } else if (value instanceof Date) {
            Date dateValue = (Date) value;
            String formatted = dateFormatFor(dateValue).format(dateValue);
            sb.append(formatted);
        } else {
            throw new DataException("Failed to serialize unexpected value type " + value.getClass().getName() + ": " + value);
        }
    }

    protected static void appendIterable(StringBuilder sb, Iterator<?> iter) {
        if (iter.hasNext()) {
            append(sb, iter.next(), true);
            while (iter.hasNext()) {
                sb.append(',');
                append(sb, iter.next(), true);
            }
        }
    }

    protected static String escape(String value) {
        String replace1 = TWO_BACKSLASHES.matcher(value).replaceAll("\\\\\\\\");
        return DOUBLEQOUTE.matcher(replace1).replaceAll("\\\\\"");
    }

    public static DateFormat dateFormatFor(java.util.Date value) {
        if (value.getTime() < MILLIS_PER_DAY) {
            return new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN);
        }
        if (value.getTime() % MILLIS_PER_DAY == 0) {
            return new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN);
        }
        return new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN);
    }

    protected static boolean canParseSingleTokenLiteral(Parser parser, boolean embedded, String tokenLiteral) {
        int startPosition = parser.mark();
        // If the next token is what we expect, then either...
        if (parser.canConsume(tokenLiteral)) {
            //   ...we're reading an embedded value, in which case the next token will be handled appropriately
            //      by the caller if it's something like an end delimiter for a map or array, or a comma to
            //      separate multiple embedded values...
            //   ...or it's being parsed as part of a top-level string, in which case, any other tokens should
            //      cause use to stop parsing this single-token literal as such and instead just treat it like
            //      a string. For example, the top-level string "true}" will be tokenized as the tokens "true" and
            //      "}", but should ultimately be parsed as just the string "true}" instead of the boolean true.
            if (embedded || !parser.hasNext()) {
                return true;
            }
        }
        parser.rewindTo(startPosition);
        return false;
    }

    protected static io.ctsi.tenet.kafka.connect.data.SchemaAndValue parse(Parser parser, boolean embedded) throws NoSuchElementException {
        if (!parser.hasNext()) {
            return null;
        }
        if (embedded) {
            if (parser.canConsume(QUOTE_DELIMITER)) {
                StringBuilder sb = new StringBuilder();
                while (parser.hasNext()) {
                    if (parser.canConsume(QUOTE_DELIMITER)) {
                        break;
                    }
                    sb.append(parser.next());
                }
                String content = sb.toString();
                // We can parse string literals as temporal logical types, but all others
                // are treated as strings
                io.ctsi.tenet.kafka.connect.data.SchemaAndValue parsed = parseString(content);
                if (parsed != null && TEMPORAL_LOGICAL_TYPE_NAMES.contains(parsed.schema().name())) {
                    return parsed;
                }
                return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.STRING_SCHEMA, content);
            }
        }

        if (canParseSingleTokenLiteral(parser, embedded, NULL_VALUE)) {
            return null;
        }
        if (canParseSingleTokenLiteral(parser, embedded, TRUE_LITERAL)) {
            return TRUE_SCHEMA_AND_VALUE;
        }
        if (canParseSingleTokenLiteral(parser, embedded, FALSE_LITERAL)) {
            return FALSE_SCHEMA_AND_VALUE;
        }

        int startPosition = parser.mark();

        try {
            if (parser.canConsume(ARRAY_BEGIN_DELIMITER)) {
                List<Object> result = new ArrayList<>();
                io.ctsi.tenet.kafka.connect.data.Schema elementSchema = null;
                while (parser.hasNext()) {
                    if (parser.canConsume(ARRAY_END_DELIMITER)) {
                        io.ctsi.tenet.kafka.connect.data.Schema listSchema;
                        if (elementSchema != null) {
                            listSchema = SchemaBuilder.array(elementSchema).schema();
                            result = alignListEntriesWithSchema(listSchema, result);
                        } else {
                            // Every value is null
                            listSchema = SchemaBuilder.arrayOfNull().build();
                        }
                        return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(listSchema, result);
                    }

                    if (parser.canConsume(COMMA_DELIMITER)) {
                        throw new DataException("Unable to parse an empty array element: " + parser.original());
                    }
                    io.ctsi.tenet.kafka.connect.data.SchemaAndValue element = parse(parser, true);
                    elementSchema = commonSchemaFor(elementSchema, element);
                    result.add(element != null ? element.value() : null);

                    int currentPosition = parser.mark();
                    if (parser.canConsume(ARRAY_END_DELIMITER)) {
                        parser.rewindTo(currentPosition);
                    } else if (!parser.canConsume(COMMA_DELIMITER)) {
                        throw new DataException("Array elements missing '" + COMMA_DELIMITER + "' delimiter");
                    }
                }

                // Missing either a comma or an end delimiter
                if (COMMA_DELIMITER.equals(parser.previous())) {
                    throw new DataException("Array is missing element after ',': " + parser.original());
                }
                throw new DataException("Array is missing terminating ']': " + parser.original());
            }

            if (parser.canConsume(MAP_BEGIN_DELIMITER)) {
                Map<Object, Object> result = new LinkedHashMap<>();
                io.ctsi.tenet.kafka.connect.data.Schema keySchema = null;
                io.ctsi.tenet.kafka.connect.data.Schema valueSchema = null;
                while (parser.hasNext()) {
                    if (parser.canConsume(MAP_END_DELIMITER)) {
                        io.ctsi.tenet.kafka.connect.data.Schema mapSchema;
                        if (keySchema != null && valueSchema != null) {
                            mapSchema = SchemaBuilder.map(keySchema, valueSchema).build();
                            result = alignMapKeysAndValuesWithSchema(mapSchema, result);
                        } else if (keySchema != null) {
                            mapSchema = SchemaBuilder.mapWithNullValues(keySchema);
                            result = alignMapKeysWithSchema(mapSchema, result);
                        } else {
                            mapSchema = SchemaBuilder.mapOfNull().build();
                        }
                        return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(mapSchema, result);
                    }

                    if (parser.canConsume(COMMA_DELIMITER)) {
                        throw new DataException("Unable to parse a map entry with no key or value: " + parser.original());
                    }
                    io.ctsi.tenet.kafka.connect.data.SchemaAndValue key = parse(parser, true);
                    if (key == null || key.value() == null) {
                        throw new DataException("Map entry may not have a null key: " + parser.original());
                    }

                    if (!parser.canConsume(ENTRY_DELIMITER)) {
                        throw new DataException("Map entry is missing '" + ENTRY_DELIMITER
                                + "' at " + parser.position()
                                + " in " + parser.original());
                    }
                    io.ctsi.tenet.kafka.connect.data.SchemaAndValue value = parse(parser, true);
                    Object entryValue = value != null ? value.value() : null;
                    result.put(key.value(), entryValue);

                    parser.canConsume(COMMA_DELIMITER);
                    keySchema = commonSchemaFor(keySchema, key);
                    valueSchema = commonSchemaFor(valueSchema, value);
                }
                // Missing either a comma or an end delimiter
                if (COMMA_DELIMITER.equals(parser.previous())) {
                    throw new DataException("Map is missing element after ',': " + parser.original());
                }
                throw new DataException("Map is missing terminating '}': " + parser.original());
            }
        } catch (DataException e) {
            LOG.trace("Unable to parse the value as a map or an array; reverting to string", e);
            parser.rewindTo(startPosition);
        }

        String token = parser.next();
        if (token.trim().isEmpty()) {
            return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.STRING_SCHEMA, token);
        }
        token = token.trim();

        char firstChar = token.charAt(0);
        boolean firstCharIsDigit = Character.isDigit(firstChar);

        // Temporal types are more restrictive, so try them first
        if (firstCharIsDigit) {
            // The time and timestamp literals may be split into 5 tokens since an unescaped colon
            // is a delimiter. Check these first since the first of these tokens is a simple numeric
            int position = parser.mark();
            String remainder = parser.next(4);
            if (remainder != null) {
                String timeOrTimestampStr = token + remainder;
                io.ctsi.tenet.kafka.connect.data.SchemaAndValue temporal = parseAsTemporal(timeOrTimestampStr);
                if (temporal != null) {
                    return temporal;
                }
            }
            // No match was found using the 5 tokens, so rewind and see if the current token has a date, time, or timestamp
            parser.rewindTo(position);
            io.ctsi.tenet.kafka.connect.data.SchemaAndValue temporal = parseAsTemporal(token);
            if (temporal != null) {
                return temporal;
            }
        }
        if (firstCharIsDigit || firstChar == '+' || firstChar == '-') {
            try {
                // Try to parse as a number ...
                BigDecimal decimal = new BigDecimal(token);
                try {
                    return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.INT8_SCHEMA, decimal.byteValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.INT16_SCHEMA, decimal.shortValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.INT32_SCHEMA, decimal.intValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.INT64_SCHEMA, decimal.longValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                float fValue = decimal.floatValue();
                if (fValue != Float.NEGATIVE_INFINITY && fValue != Float.POSITIVE_INFINITY
                        && decimal.scale() != 0) {
                    return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.FLOAT32_SCHEMA, fValue);
                }
                double dValue = decimal.doubleValue();
                if (dValue != Double.NEGATIVE_INFINITY && dValue != Double.POSITIVE_INFINITY
                        && decimal.scale() != 0) {
                    return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.FLOAT64_SCHEMA, dValue);
                }
                io.ctsi.tenet.kafka.connect.data.Schema schema = io.ctsi.tenet.kafka.connect.data.Decimal.schema(decimal.scale());
                return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(schema, decimal);
            } catch (NumberFormatException e) {
                // can't parse as a number
            }
        }
        if (embedded) {
            throw new DataException("Failed to parse embedded value");
        }
        // At this point, the only thing this non-embedded value can be is a string.
        return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Schema.STRING_SCHEMA, parser.original());
    }

    private static io.ctsi.tenet.kafka.connect.data.SchemaAndValue parseAsTemporal(String token) {
        if (token == null) {
            return null;
        }
        // If the colons were escaped, we'll see the escape chars and need to remove them
        token = token.replace("\\:", ":");
        int tokenLength = token.length();
        if (tokenLength == ISO_8601_TIME_LENGTH) {
            try {
                return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Time.SCHEMA, new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN).parse(token));
            } catch (ParseException e) {
                // not a valid date
            }
        } else if (tokenLength == ISO_8601_TIMESTAMP_LENGTH) {
            try {
                return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(io.ctsi.tenet.kafka.connect.data.Timestamp.SCHEMA, new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(token));
            } catch (ParseException e) {
                // not a valid date
            }
        } else if (tokenLength == ISO_8601_DATE_LENGTH) {
            try {
                return new io.ctsi.tenet.kafka.connect.data.SchemaAndValue(Date.SCHEMA, new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN).parse(token));
            } catch (ParseException e) {
                // not a valid date
            }
        }
        return null;
    }

    protected static io.ctsi.tenet.kafka.connect.data.Schema commonSchemaFor(io.ctsi.tenet.kafka.connect.data.Schema previous, io.ctsi.tenet.kafka.connect.data.SchemaAndValue latest) {
        if (latest == null) {
            return previous;
        }
        if (previous == null) {
            return latest.schema();
        }
        io.ctsi.tenet.kafka.connect.data.Schema newSchema = latest.schema();
        io.ctsi.tenet.kafka.connect.data.Schema.Type previousType = previous.type();
        io.ctsi.tenet.kafka.connect.data.Schema.Type newType = newSchema.type();
        if (previousType != newType) {
            switch (previous.type()) {
                case INT8:
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT16 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT32 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT64 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT32 || newType ==
                            io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case INT16:
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT8) {
                        return previous;
                    }
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT32 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT64 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT32 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case INT32:
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT8 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT16) {
                        return previous;
                    }
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT64 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT32 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case INT64:
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT8 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT16 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT32) {
                        return previous;
                    }
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT32 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case FLOAT32:
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT8 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT16 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT32 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT64) {
                        return previous;
                    }
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case FLOAT64:
                    if (newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT8 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT16 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT32 || newType == io.ctsi.tenet.kafka.connect.data.Schema.Type.INT64 || newType ==
                            io.ctsi.tenet.kafka.connect.data.Schema.Type.FLOAT32) {
                        return previous;
                    }
                    break;
            }
            return null;
        }
        if (previous.isOptional() == newSchema.isOptional()) {
            // Use the optional one
            return previous.isOptional() ? previous : newSchema;
        }
        if (!previous.equals(newSchema)) {
            return null;
        }
        return previous;
    }

    protected static List<Object> alignListEntriesWithSchema(io.ctsi.tenet.kafka.connect.data.Schema schema, List<Object> input) {
        io.ctsi.tenet.kafka.connect.data.Schema valueSchema = schema.valueSchema();
        List<Object> result = new ArrayList<>();
        for (Object value : input) {
            Object newValue = convertTo(valueSchema, null, value);
            result.add(newValue);
        }
        return result;
    }

    protected static Map<Object, Object> alignMapKeysAndValuesWithSchema(io.ctsi.tenet.kafka.connect.data.Schema mapSchema, Map<Object, Object> input) {
        io.ctsi.tenet.kafka.connect.data.Schema keySchema = mapSchema.keySchema();
        io.ctsi.tenet.kafka.connect.data.Schema valueSchema = mapSchema.valueSchema();
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            Object newKey = convertTo(keySchema, null, entry.getKey());
            Object newValue = convertTo(valueSchema, null, entry.getValue());
            result.put(newKey, newValue);
        }
        return result;
    }

    protected static Map<Object, Object> alignMapKeysWithSchema(io.ctsi.tenet.kafka.connect.data.Schema mapSchema, Map<Object, Object> input) {
        io.ctsi.tenet.kafka.connect.data.Schema keySchema = mapSchema.keySchema();
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            Object newKey = convertTo(keySchema, null, entry.getKey());
            result.put(newKey, entry.getValue());
        }
        return result;
    }

    protected static class SchemaDetector {
        private io.ctsi.tenet.kafka.connect.data.Schema.Type knownType = null;
        private boolean optional = false;

        public SchemaDetector() {
        }

        public boolean canDetect(Object value) {
            if (value == null) {
                optional = true;
                return true;
            }
            io.ctsi.tenet.kafka.connect.data.Schema schema = inferSchema(value);
            if (schema == null) {
                return false;
            }
            if (knownType == null) {
                knownType = schema.type();
            } else if (knownType != schema.type()) {
                return false;
            }
            return true;
        }

        public io.ctsi.tenet.kafka.connect.data.Schema schema() {
            SchemaBuilder builder = SchemaBuilder.type(knownType);
            if (optional) {
                builder.optional();
            }
            return builder.schema();
        }
    }

    protected static class Parser {
        private final String original;
        private final CharacterIterator iter;
        private String nextToken = null;
        private String previousToken = null;

        public Parser(String original) {
            this.original = original;
            this.iter = new StringCharacterIterator(this.original);
        }

        public int position() {
            return iter.getIndex();
        }

        public int mark() {
            return iter.getIndex() - (nextToken != null ? nextToken.length() : 0);
        }

        public void rewindTo(int position) {
            iter.setIndex(position);
            nextToken = null;
            previousToken = null;
        }

        public String original() {
            return original;
        }

        public boolean hasNext() {
            return nextToken != null || canConsumeNextToken();
        }

        protected boolean canConsumeNextToken() {
            return iter.getEndIndex() > iter.getIndex();
        }

        public String next() {
            if (nextToken != null) {
                previousToken = nextToken;
                nextToken = null;
            } else {
                previousToken = consumeNextToken();
            }
            return previousToken;
        }

        public String next(int n) {
            int current = mark();
            int start = mark();
            for (int i = 0; i != n; ++i) {
                if (!hasNext()) {
                    rewindTo(start);
                    return null;
                }
                next();
            }
            return original.substring(current, position());
        }

        private String consumeNextToken() throws NoSuchElementException {
            boolean escaped = false;
            int start = iter.getIndex();
            char c = iter.current();
            while (canConsumeNextToken()) {
                switch (c) {
                    case '\\':
                        escaped = !escaped;
                        break;
                    case ':':
                    case ',':
                    case '{':
                    case '}':
                    case '[':
                    case ']':
                    case '\"':
                        if (!escaped) {
                            if (start < iter.getIndex()) {
                                // Return the previous token
                                return original.substring(start, iter.getIndex());
                            }
                            // Consume and return this delimiter as a token
                            iter.next();
                            return original.substring(start, start + 1);
                        }
                        // escaped, so continue
                        escaped = false;
                        break;
                    default:
                        // If escaped, then we don't care what was escaped
                        escaped = false;
                        break;
                }
                c = iter.next();
            }
            return original.substring(start, iter.getIndex());
        }

        public String previous() {
            return previousToken;
        }

        public boolean canConsume(String expected) {
            return canConsume(expected, true);
        }

        public boolean canConsume(String expected, boolean ignoreLeadingAndTrailingWhitespace) {
            if (isNext(expected, ignoreLeadingAndTrailingWhitespace)) {
                // consume this token ...
                nextToken = null;
                return true;
            }
            return false;
        }

        protected boolean isNext(String expected, boolean ignoreLeadingAndTrailingWhitespace) {
            if (nextToken == null) {
                if (!hasNext()) {
                    return false;
                }
                // There's another token, so consume it
                nextToken = consumeNextToken();
            }
            if (ignoreLeadingAndTrailingWhitespace) {
                while (nextToken.trim().isEmpty() && canConsumeNextToken()) {
                    nextToken = consumeNextToken();
                }
            }
            return ignoreLeadingAndTrailingWhitespace
                    ? nextToken.trim().equals(expected)
                    : nextToken.equals(expected);
        }
    }
}

