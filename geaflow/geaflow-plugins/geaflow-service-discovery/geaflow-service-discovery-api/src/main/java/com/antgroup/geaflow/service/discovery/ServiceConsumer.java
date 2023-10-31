package com.antgroup.geaflow.service.discovery;

public interface ServiceConsumer {

    /**
     * Check if the specified path exists.
     */
    boolean exists(String path);

    /**
     * Get the data at the specified path and set a watch.
     */
    byte[] getDataAndWatch(String path);

    /**
     * Register the specified listener to receive updated events.
     */
    void register(ServiceListener listener);

    /**
     * close the consumer.
     */
    void close();
}
