package com.antgroup.geaflow.service.discovery;

public interface ServiceProvider extends ServiceConsumer {

    /**
     * Watch the specified path for updated events.
     * If the node already exists, the method returns true. If the node does not exist, the method returns false.
     */
    boolean watchAndCheckExists(String path);

    /**
     * Delete the specified path.
     */
    void delete(String path);

    /**
     * Creates the specified node with the specified data and watches it.
     */
    boolean createAndWatch(String path, byte[] data);

    /**
     * Update the specified node with the specified data.
     */
    boolean update(String path, byte[] data);

}
