package com.antgroup.geaflow.store;

/**
 * IStateful store is stateful, which means it ensure data HA and can be recovered.
 */
public interface IStatefulStore extends IBaseStore {

    /**
     * make a snapshot and ensure data HA.
     */
    void archive(long checkpointId);

    /**
     * recover the store data.
     */
    void recovery(long checkpointId);

    /**
     * recover the latest store data.
     */
    long recoveryLatest();

    /**
     * trigger manual store data compaction.
     */
    void compact();

    /**
     * delete the store data.
     */
    void drop();
}
