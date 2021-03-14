package io.ctsi.tenet.kafka.infrastructure.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(TenetKafkaTaskConfigurationSelector.class)
@Deprecated
public @interface TenetKafka {
}
