package com.antgroup.geaflow.service.discovery;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SERVICE_DISCOVERY_TYPE;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.store.redis.RedisConfigKeys;
import com.github.fppt.jedismock.RedisServer;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RedisTest {

    private static final String MASTER = "master";
    private RedisServer redisServer;
    private Configuration configuration;
    private ServiceBuilder serviceBuilder;
    private ServiceConsumer consumer;
    private ServiceProvider provider;
    private String serviceType = "redis";

    @BeforeClass
    public void prepare() throws IOException {
        redisServer = RedisServer.newRedisServer().start();
        this.configuration = new Configuration();
        this.configuration.put(RedisConfigKeys.REDIS_HOST, redisServer.getHost());
        this.configuration.put(RedisConfigKeys.REDIS_PORT, String.valueOf(redisServer.getBindPort()));
        this.configuration.put(SERVICE_DISCOVERY_TYPE, "redis");
        this.configuration.put(ExecutionConfigKeys.JOB_APP_NAME, "testJob123");
    }

    @AfterClass
    public void tearUp() throws IOException {
        if (consumer != null) {
            consumer.close();
        }
        if (provider != null) {
            provider.close();
        }
        redisServer.stop();
    }

    @Test
    public void testRedisServiceBuilder() {
        serviceBuilder = ServiceBuilderFactory.build(
            configuration.getString(SERVICE_DISCOVERY_TYPE));
        Assert.assertTrue(serviceBuilder instanceof RedisServiceBuilder);
        this.consumer = ServiceBuilderFactory.build(serviceType).buildConsumer(configuration);
        Assert.assertTrue(consumer instanceof RedisServiceConsumer);
        this.provider = ServiceBuilderFactory.build(serviceType).buildProvider(configuration);
        Assert.assertTrue(provider instanceof RedisServiceProvider);
    }

    @Test
    public void testCreateBaseNode() {
        this.provider = ServiceBuilderFactory.build(serviceType).buildProvider(configuration);
        Assert.assertTrue(provider.exists(""));
    }

    @Test
    public void testDelete() {
        this.provider = ServiceBuilderFactory.build(serviceType).buildProvider(configuration);
        boolean res = provider.createAndWatch(MASTER, "123".getBytes());
        Assert.assertTrue(res);
        Assert.assertTrue(provider.exists(MASTER));
        this.provider.delete(MASTER);
        Assert.assertFalse(provider.exists(MASTER));
    }

    @Test
    public void testConsumer() {
        this.provider = ServiceBuilderFactory.build(serviceType).buildProvider(configuration);
        ServiceConsumer consumer = ServiceBuilderFactory.build(serviceType).buildConsumer(configuration);
        boolean res = provider.createAndWatch(MASTER, "123".getBytes());
        Assert.assertTrue(res);
        byte[] datas = consumer.getDataAndWatch(MASTER);
        Assert.assertEquals(datas, "123".getBytes());
        provider.delete(MASTER);
        consumer.close();
    }

    @Test
    public void testUpdate() {
        String version = "version";
        this.provider = ServiceBuilderFactory.build(serviceType).buildProvider(configuration);
        ServiceConsumer consumer = ServiceBuilderFactory.build(serviceType)
            .buildConsumer(configuration);

        Assert.assertFalse(consumer.exists(version));
        byte[] versionData = Longs.toByteArray(2);

        this.provider.update(version, versionData);
        byte[] data = consumer.getDataAndWatch(version);
        Assert.assertEquals(versionData, data);

        versionData = Longs.toByteArray(3);
        this.provider.update(version, versionData);
        data = consumer.getDataAndWatch(version);
        Assert.assertEquals(versionData, data);
        consumer.close();
    }

    @Test
    public void testBaseKey() {
        Map<String, String> config = configuration.getConfigMap();
        Configuration newConfig = new Configuration(new HashMap<>(config));
        newConfig.put(ExecutionConfigKeys.JOB_APP_NAME, "234");
        this.consumer = ServiceBuilderFactory.build(serviceType).buildConsumer(newConfig);
        this.provider = ServiceBuilderFactory.build(serviceType).buildProvider(newConfig);
        Assert.assertTrue(provider.exists(null));
        ServiceBuilderFactory.clear();
    }

}
