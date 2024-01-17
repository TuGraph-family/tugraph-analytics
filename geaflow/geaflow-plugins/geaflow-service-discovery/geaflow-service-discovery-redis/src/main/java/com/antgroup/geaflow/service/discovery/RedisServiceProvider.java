package com.antgroup.geaflow.service.discovery;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.state.serializer.DefaultKVSerializer;
import com.antgroup.geaflow.store.context.StoreContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisServiceProvider implements ServiceProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisServiceProvider.class);
    private final RecoverableRedis recoverableRedis;
    private final String baseKey;
    private final String namespace;

    public RedisServiceProvider(Configuration configuration) {
        this.recoverableRedis = new RecoverableRedis();
        String appName = configuration.getString(ExecutionConfigKeys.JOB_APP_NAME);
        this.baseKey = appName.startsWith("/") ? appName : "/" + appName;
        StoreContext storeContext = new StoreContext(baseKey);
        storeContext.withKeySerializer(new DefaultKVSerializer(String.class, null));
        storeContext.withConfig(configuration);
        this.recoverableRedis.init(storeContext);
        this.namespace = recoverableRedis.getNamespace();
        this.recoverableRedis.setData(namespace, new byte[0]);
    }

    @Override
    public boolean exists(String path) {
        if (StringUtils.isBlank(path)) {
            return this.recoverableRedis.getData(this.namespace) != null;
        }
        return this.recoverableRedis.getData(path) != null;
    }

    @Override
    public byte[] getDataAndWatch(String path) {
        return this.recoverableRedis.getData(path);
    }

    @Override
    public boolean watchAndCheckExists(String path) {
        return false;
    }

    @Override
    public void delete(String path) {
        this.recoverableRedis.deleteData(path);
    }

    @Override
    public boolean createAndWatch(String path, byte[] data) {
        this.recoverableRedis.setData(path, data);
        return true;
    }

    @Override
    public boolean update(String path, byte[] data) {
        this.recoverableRedis.setData(path, data);
        return true;
    }

    @Override
    public void close() {
        if (this.recoverableRedis != null) {
            this.recoverableRedis.close();
        }
    }

}
