package com.antgroup.geaflow.service.discovery;

import com.antgroup.geaflow.common.serialize.ISerializer;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.utils.RetryCommand;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.redis.BaseRedisStore;
import redis.clients.jedis.Jedis;

public class RecoverableRedis extends BaseRedisStore {
    private ISerializer serializer;
    private String namespace;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.serializer = SerializerFactory.getKryoSerializer();
        this.namespace = storeContext.getName() + BaseRedisStore.REDIS_NAMESPACE_SPLITTER;
    }

    public void setData(String key, byte[] valueArray) {
        byte[] keyArray = this.serializer.serialize(key);
        byte[] redisKey = getRedisKey(keyArray);
        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.set(redisKey, valueArray);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    public byte[] getData(String key) {
        byte[] keyArray = this.serializer.serialize(key);
        byte[] redisKey = getRedisKey(keyArray);
        return RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                return jedis.get(redisKey);
            }
        }, retryTimes, retryIntervalMs);
    }

    public void deleteData(String key) {
        byte[] keyArray = this.serializer.serialize(key);
        byte[] redisKey = getRedisKey(keyArray);

        RetryCommand.run(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(redisKey);
            }
            return null;
        }, retryTimes, retryIntervalMs);
    }

    public String getNamespace() {
        return this.namespace;
    }

}

