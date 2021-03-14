package io.ctsi.tenet.kafka.mongodb.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.ctsi.tenet.kafka.mongodb.sink.MongoSinkConfig.TOPIC_OVERRIDE_DOC;
import static io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig.FULLY_QUALIFIED_CLASS_NAME;
import static java.lang.String.format;

public class Validators {
    public interface ValidatorWithOperators extends ConfigDef.Validator {
        default ValidatorWithOperators or(final ValidatorWithOperators other) {
            return withStringDef(
                    format("%s OR %s", this.toString(), other.toString()),
                    (name, value) -> {
                        try {
                            this.ensureValid(name, value);
                        } catch (ConfigException e) {
                            other.ensureValid(name, value);
                        }
                    });
        }
    }

    public static ValidatorWithOperators emptyString() {
        return withStringDef(
                "An empty string",
                (name, value) -> {
                    // value type already validated when parsed as String, hence ignoring ClassCastException
                    if (!((String) value).isEmpty()) {
                        throw new ConfigException(name, value, "Not empty");
                    }
                });
    }

    public static ValidatorWithOperators matching(final Pattern pattern) {
        return withStringDef(
                format("A string matching `%s`", pattern),
                (name, value) -> matchPattern(pattern, name, (String) value));
    }

    @SuppressWarnings("unchecked")
    public static ValidatorWithOperators listMatchingPattern(final Pattern pattern) {
        return withStringDef(
                format("A list matching: `%s`", pattern),
                (name, value) -> {
                    try {
                        ((List) value).forEach(v -> matchPattern(pattern, name, (String) v));
                    } catch (ConnectConfigException e) {
                        throw new ConfigException(name, value, e.getOriginalMessage());
                    }
                });
    }

    private static void matchPattern(final Pattern pattern, final String name, final String value) {
        if (!pattern.matcher(value).matches()) {
            String message = "Does not match: " + pattern.pattern();
            if (pattern.equals(FULLY_QUALIFIED_CLASS_NAME)) {
                message = "Does not match expected class pattern.";
            }
            throw new ConnectConfigException(name, value, message);
        }
    }

    public static ValidatorWithOperators isAValidRegex() {
        return withStringDef(
                "A valid regex",
                ((name, value) -> {
                    try {
                        Pattern.compile((String) value);
                    } catch (Exception e) {
                        throw new ConfigException(name, value, "Invalid regex: " + e.getMessage());
                    }
                }));
    }

    public static ValidatorWithOperators topicOverrideValidator() {
        return withStringDef(
                "Topic override",
                (name, value) -> {
                    if (!((String) value).isEmpty()) {
                        throw new ConfigException(
                                name,
                                value,
                                "This configuration shouldn't be set directly. See the documentation about how to "
                                        + "configure topic based overrides.\n"
                                        + TOPIC_OVERRIDE_DOC);
                    }
                });
    }

    public static ValidatorWithOperators errorCheckingValueValidator(
            final String validValuesString, final Consumer<String> consumer) {
        return withStringDef(
                validValuesString,
                ((name, value) -> {
                    try {
                        consumer.accept((String) value);
                    } catch (Exception e) {
                        throw new ConfigException(name, value, e.getMessage());
                    }
                }));
    }

    public static ValidatorWithOperators withStringDef(
            final String validatorString, final ConfigDef.Validator validator) {
        return new ValidatorWithOperators() {
            @Override
            public void ensureValid(final String name, final Object value) {
                validator.ensureValid(name, value);
            }

            @Override
            public String toString() {
                return validatorString;
            }
        };
    }

    public static final class EnumValidatorAndRecommender
            implements ValidatorWithOperators, ConfigDef.Recommender {
        private final List<String> values;

        private EnumValidatorAndRecommender(final List<String> values) {
            this.values = values;
        }

        public static <E> EnumValidatorAndRecommender in(final E[] enumerators) {
            return in(enumerators, Object::toString);
        }

        public static <E> EnumValidatorAndRecommender in(
                final E[] enumerators, final Function<E, String> mapper) {
            final List<String> values = new ArrayList<>(enumerators.length);
            for (E e : enumerators) {
                values.add(mapper.apply(e).toLowerCase(Locale.ROOT));
            }
            return new EnumValidatorAndRecommender(values);
        }

        @Override
        public void ensureValid(final String key, final Object value) {
            String enumValue = (String) value;
            if (!values.contains(enumValue.toLowerCase(Locale.ROOT))) {
                throw new ConfigException(
                        key, value, format("Invalid enumerator value. Should be one of: %s", values));
            }
        }

        @Override
        public String toString() {
            return values.toString();
        }

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
            return new ArrayList<>(values);
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private Validators() {}
}
