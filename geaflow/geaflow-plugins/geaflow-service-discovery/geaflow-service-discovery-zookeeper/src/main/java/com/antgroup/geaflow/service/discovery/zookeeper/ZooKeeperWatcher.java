package com.antgroup.geaflow.service.discovery.zookeeper;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.service.discovery.ServiceListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperWatcher implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperWatcher.class);

    private final Configuration conf;
    public String baseZNode;

    private final List<ServiceListener> listeners = new CopyOnWriteArrayList<ServiceListener>();
    private final RecoverableZooKeeper zooKeeper;

    public ZooKeeperWatcher(Configuration conf) {
        this(conf, false);
    }

    public ZooKeeperWatcher(Configuration conf, boolean canCreateBaseZNode) {
        this.conf = conf;

        try {
            this.zooKeeper = ZKUtil.connect(conf, this);
            String jobName = conf.getString(ExecutionConfigKeys.JOB_APP_NAME);
            baseZNode = conf.getString(ZooKeeperConfigKeys.ZOOKEEPER_BASE_NODE, "/" + jobName);
            LOGGER.info("zk node {}", baseZNode);
            if (canCreateBaseZNode) {
                createBaseZNodes();
            }
        } catch (Exception t) {
            LOGGER.error("watcher init failed", t);
            close();
            throw new GeaflowRuntimeException(t);
        }
    }

    @Override
    public void process(WatchedEvent event) {

        switch (event.getType()) {

            // Otherwise pass along to the listeners
            case NodeCreated: {
                for (ServiceListener listener : listeners) {
                    listener.nodeCreated(event.getPath());
                }
                break;
            }

            case NodeDeleted: {
                for (ServiceListener listener : listeners) {
                    listener.nodeDeleted(event.getPath());
                }
                break;
            }
            case NodeDataChanged: {
                for (ServiceListener listener : listeners) {
                    listener.nodeDataChanged(event.getPath());
                }
                break;
            }
            default:
                break;
        }

    }

    protected void createBaseZNodes() {
        try {
            // Create all the necessary "directories" of znodes
            ZKUtil.createPersistentNode(this, baseZNode);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Unexpected KeeperException creating base node", e);
        }
    }

    public void close() {
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            LOGGER.error("close exception", e);
        }
    }


    public void registerListener(ServiceListener listener) {
        listeners.add(listener);
    }

    public RecoverableZooKeeper getRecoverableZooKeeper() {
        return zooKeeper;
    }

}
