package io.ctsi.tenet.kafka.connect.transforms;

import io.ctsi.tenet.kafka.connect.ConnectRecord;
import io.ctsi.tenet.kafka.connect.policy.RetryWithToleranceOperator;
import io.ctsi.tenet.kafka.connect.policy.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class TransformationChain<R extends ConnectRecord<R>> {
    private static final Logger log = LoggerFactory.getLogger(TransformationChain.class);

    private final List<Transformation<R>> transformations;
    private final RetryWithToleranceOperator retryWithToleranceOperator;

    public TransformationChain(List<Transformation<R>> transformations, RetryWithToleranceOperator retryWithToleranceOperator) {
        this.transformations = transformations;
        this.retryWithToleranceOperator = retryWithToleranceOperator;
    }

    public R apply(R record) {
        if (transformations.isEmpty()) return record;

        for (final Transformation<R> transformation : transformations) {
            final R current = record;

            log.trace("Applying transformation {} to {}",
                    transformation.getClass().getName(), record);
            // execute the operation
            record = retryWithToleranceOperator.execute(() -> transformation.apply(current), Stage.TRANSFORMATION, transformation.getClass());

            if (record == null) break;
        }

        return record;
    }

    public void close() {
        for (Transformation<R> transformation : transformations) {
            transformation.close();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransformationChain that = (TransformationChain) o;
        return Objects.equals(transformations, that.transformations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformations);
    }

    public String toString() {
        StringJoiner chain = new StringJoiner(", ", getClass().getName() + "{", "}");
        for (Transformation<R> transformation : transformations) {
            chain.add(transformation.getClass().getName());
        }
        return chain.toString();
    }
}