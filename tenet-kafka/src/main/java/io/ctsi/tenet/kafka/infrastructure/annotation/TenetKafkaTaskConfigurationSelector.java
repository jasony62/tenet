package io.ctsi.tenet.kafka.infrastructure.annotation;

import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.annotation.Order;
import org.springframework.core.type.AnnotationMetadata;

@Order
public class TenetKafkaTaskConfigurationSelector implements DeferredImportSelector {

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        return new String[] { io.ctsi.tenet.kafka.infrastructure.annotation.TenetKafkaBootstrapConfiguration.class.getName() };
    }

}