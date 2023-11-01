package com.antgroup.geaflow.service.discovery;

public interface ServiceListener {

    /**
     * Called when a new node has been created.
     */
    void nodeCreated(String path);

    /**
     * Called when a node has been deleted.
     */
    void nodeDeleted(String path);

    /**
     * Called when an existing node has changed data.
     */
    void nodeDataChanged(String path);

    /**
     * Called when an existing node has a child node added or removed.
     */
    void nodeChildrenChanged(String path);
}
