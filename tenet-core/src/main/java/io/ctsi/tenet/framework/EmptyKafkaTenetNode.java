package io.ctsi.tenet.framework;

import java.util.function.Function;

/**
 * @author Mc.D
 */
public class EmptyKafkaTenetNode extends  io.ctsi.tenet.framework.AbstractKafkaTenetNode {

    @Override
    public Function<  io.ctsi.tenet.framework.TenetMessage, Integer> getBefore() {
        return null;
    }

    @Override
    public Function<  io.ctsi.tenet.framework.TenetMessage, Integer> getRun() {
        return null;
    }

    @Override
    public Function<  io.ctsi.tenet.framework.TenetMessage, Integer> getAfter() {
        return null;
    }
}
