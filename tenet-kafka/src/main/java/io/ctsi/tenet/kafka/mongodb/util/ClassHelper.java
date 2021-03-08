package io.ctsi.tenet.kafka.mongodb.util;

import java.util.List;

import static java.lang.String.format;

public class ClassHelper {
    @SuppressWarnings("unchecked")
    public static <T> T createInstance(
            final String configKey, final String className, final Class<T> clazz) {
        return createInstance(
                configKey,
                className,
                clazz,
                () -> (T) Class.forName(className).getConstructor().newInstance());
    }

    @SuppressWarnings("unchecked")
    public static <T> T createInstance(
            final String configKey,
            final String className,
            final Class<T> clazz,
            final List<Class<?>> constructorArgs,
            final List<Object> initArgs) {
        return createInstance(
                configKey,
                className,
                clazz,
                () ->
                        (T)
                                Class.forName(className)
                                        .getConstructor(constructorArgs.toArray(new Class<?>[0]))
                                        .newInstance(initArgs.toArray(new Object[0])));
    }

    public static <T> T createInstance(
            final String configKey,
            final String className,
            final Class<T> clazz,
            final ClassCreator<T> cc) {
        try {
            return cc.init();
        } catch (ClassCastException e) {
            throw new ConnectConfigException(
                    configKey,
                    className,
                    format("Contract violation class doesn't implement: '%s'", clazz.getSimpleName()));
        } catch (ClassNotFoundException e) {
            throw new ConnectConfigException(
                    configKey, className, format("Class not found: %s", e.getMessage()));
        } catch (Exception e) {
            if (e.getCause() instanceof ConnectConfigException) {
                throw (ConnectConfigException) e.getCause();
            }
            throw new ConnectConfigException(configKey, className, e.getMessage());
        }
    }

    @FunctionalInterface
    public interface ClassCreator<T> {
        T init() throws Exception;
    }

    private ClassHelper() {}
}
