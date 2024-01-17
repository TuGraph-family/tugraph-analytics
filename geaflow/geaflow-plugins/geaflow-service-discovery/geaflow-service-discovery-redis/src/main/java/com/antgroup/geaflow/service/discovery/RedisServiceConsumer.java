package com.antgroup.geaflow.service.discovery;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.state.serializer.DefaultKVSerializer;
import com.antgroup.geaflow.store.context.StoreContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisServiceConsumer implements ServiceConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisServiceConsumer.class);

    private final RecoverableRedis recoverableRedis;

    private final String baseKey;

    private final String namespace;

    public RedisServiceConsumer(Configuration configuration) {
        this.recoverableRedis = new RecoverableRedis();
        String appName = configuration.getString(ExecutionConfigKeys.JOB_APP_NAME);
        this.baseKey = appName.startsWith("/") ? appName : "/" + appName;
        StoreContext storeContext = new StoreContext(baseKey);
        storeContext.withKeySerializer(new DefaultKVSerializer(String.class, null));
        storeContext.withConfig(configuration);
        this.recoverableRedis.init(storeContext);
        this.namespace = recoverableRedis.getNamespace();
        LOGGER.info("redis service consumer base key is {}, namespace is {}", this.baseKey, this.namespace);
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
    public void close() {
        if (this.recoverableRedis != null) {
            this.recoverableRedis.close();
        }
    }

}
