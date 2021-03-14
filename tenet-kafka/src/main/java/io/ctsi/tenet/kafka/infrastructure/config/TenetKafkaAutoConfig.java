package io.ctsi.tenet.kafka.infrastructure.config;

import io.ctsi.tenet.kafka.config.SchemaDescProperties;
import io.ctsi.tenet.kafka.config.TenetKafkaProperties;
import io.ctsi.tenet.kafka.config.TenetSinkProperties;
import io.ctsi.tenet.kafka.infrastructure.annotation.TenetKafkaTaskConfigurationSelector;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@EnableConfigurationProperties({SchemaDescProperties.class, TenetKafkaProperties.class, TenetSinkProperties.class})
@Import(TenetKafkaTaskConfigurationSelector.class)
public class TenetKafkaAutoConfig {
}
