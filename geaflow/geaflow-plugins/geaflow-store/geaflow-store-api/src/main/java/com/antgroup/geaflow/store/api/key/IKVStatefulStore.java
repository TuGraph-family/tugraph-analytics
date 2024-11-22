package com.antgroup.geaflow.store.api.key;

import com.antgroup.geaflow.store.IStatefulStore;

public interface IKVStatefulStore<K, V> extends IKVStore<K, V>, IStatefulStore {

}
