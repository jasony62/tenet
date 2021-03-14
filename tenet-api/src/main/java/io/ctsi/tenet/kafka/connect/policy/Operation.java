package io.ctsi.tenet.kafka.connect.policy;

import java.util.concurrent.Callable;

public interface Operation<V> extends Callable<V> {

}