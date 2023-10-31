package com.antgroup.geaflow.state.strategy.accessor;

import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.graph.IGraphMultiVersionedStore;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReadOnlyDynamicGraphAccessor<K, VV, EV> extends RWDynamicGraphAccessor<K, VV, EV> {

    private final ReadOnlyGraph<K, VV, EV> readOnlyGraph = new ReadOnlyGraph<>();
    private Lock lock = new ReentrantLock();

    @Override
    public void init(StateContext context, IStoreBuilder storeBuilder) {
        this.readOnlyGraph.init(context, storeBuilder);
    }

    @Override
    protected List<ActionType> allowActionTypes() {
        return this.readOnlyGraph.allowActionTypes();
    }

    @Override
    public void doStoreAction(int shard, ActionType actionType, ActionRequest request) {
        request.setShard(shard);
        lock.lock();
        this.readOnlyGraph.doStoreAction(shard, actionType, request);
        lock.unlock();
    }

    @Override
    public IGraphMultiVersionedStore<K, VV, EV> getStore() {
        return (IGraphMultiVersionedStore<K, VV, EV>) this.readOnlyGraph.getStore();
    }
}
