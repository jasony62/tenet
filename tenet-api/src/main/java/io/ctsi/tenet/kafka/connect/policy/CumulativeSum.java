package io.ctsi.tenet.kafka.connect.policy;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

public class CumulativeSum implements MeasurableStat {

    private double total;

    public CumulativeSum() {
        total = 0.0;
    }

    public CumulativeSum(double value) {
        total = value;
    }

    @Override
    public void record(MetricConfig config, double value, long now) {
        total += value;
    }

    @Override
    public double measure(MetricConfig config, long now) {
        return total;
    }

}

