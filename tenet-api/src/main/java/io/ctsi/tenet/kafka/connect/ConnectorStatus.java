package io.ctsi.tenet.kafka.connect;

public class ConnectorStatus extends AbstractStatus<String> {

    public ConnectorStatus(String connector, State state, String msg, String workerUrl, int generation) {
        super(connector, state, workerUrl, generation, msg);
    }

    public ConnectorStatus(String connector, State state, String workerUrl, int generation) {
        super(connector, state, workerUrl, generation, null);
    }

    public interface Listener {

        /**
         * Invoked after connector has successfully been shutdown.
         * @param connector The connector name
         */
        void onShutdown(String connector);

        /**
         * Invoked from the Connector using {@link org.apache.kafka.connect.connector.ConnectorContext#raiseError(Exception)}
         * or if either {@link org.apache.kafka.connect.connector.Connector#start(java.util.Map)} or
         * {@link org.apache.kafka.connect.connector.Connector#stop()} throw an exception.
         * Note that no shutdown event will follow after the task has been failed.
         * @param connector The connector name
         * @param cause Error raised from the connector.
         */
        void onFailure(String connector, Throwable cause);

        /**
         * Invoked when the connector is paused through the REST API
         * @param connector The connector name
         */
        void onPause(String connector);

        /**
         * Invoked after the connector has been resumed.
         * @param connector The connector name
         */
        void onResume(String connector);

        /**
         * Invoked after successful startup of the connector.
         * @param connector The connector name
         */
        void onStartup(String connector);

        /**
         * Invoked when the connector is deleted through the REST API.
         * @param connector The connector name
         */
        void onDeletion(String connector);

    }
}