package org.apache.inlong.agent.core.ha.zk;

import com.alibaba.fastjson.JSONObject;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


public class ConfigDelegateImpl implements ConfigDelegate {
    
    /**
     * 日志
     */
    private Logger logger = LogManager.getLogger(ConfigDelegateImpl.class);

    /**
     * 默认session timeout
     */
    private static final int DEFAULT_SESSION_TIMEOUT_MS = 10 * 1000;
    /**
     * 默认connection timeout
     */
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5 * 1000;
    /**
     * max number of times to retry
     */
    private static final int DEFAULT_MAX_RETRIES = 2;
    /**
     * initial amount of time to wait between retries
     */
    private static final int DEFAULT_BASE_SLEEP_TIMEMS = 10 * 1000;
    /**
     * zk client cache
     */
    private Map<String, CuratorFramework> clientCache = new HashMap<String, CuratorFramework>();

    private Map<String, TreeCache> treeCacheMap = new HashMap<String, TreeCache>();

    private Map<String, PathChildrenCache> childrenCacheMap = new HashMap<String, PathChildrenCache>();
    
    /**
     * 构造函数
     * 
     * @param baseSleepTimeMs  initial amount of time to wait between retries
     * @param maxRetries  max number of times to retry
     * @param connectStrMap  zookeeper客户端连接字符串Map, key:zk的group, value:连接字符串
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     */
    public ConfigDelegateImpl(int baseSleepTimeMs, int maxRetries,
                              Map<String, String> connectStrMap,
                              int sessionTimeoutMs, int connectionTimeoutMs) {
        
        if (connectStrMap != null && !connectStrMap.isEmpty()) {
            for (Map.Entry<String, String> entry : connectStrMap.entrySet()) {
                String connectionString = entry.getValue();
                String group =  entry.getKey();
                    
                CuratorFramework client = initClient(baseSleepTimeMs, maxRetries, connectionString,
                    sessionTimeoutMs, connectionTimeoutMs);
                clientCache.put(group, client);

                //添加监听，连接丢失或者重连时清空缓存，防止缓存数据为脏数据
                client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState) {
                        if (ConnectionState.RECONNECTED == newState
                            || ConnectionState.LOST == newState) {
//                            ConfigCacheUtil.clearCache();
                        }
                    }
                });
            }
        } else {
            throw new IllegalArgumentException("zookeeper客户端连接字符串Map 不能为空");
        }
        
    }

    /**
     * check path is or not exist
     * @param group group
     * @param path path
     * @return true/false
     */
    @Override
    public boolean checkPathIsExist(String group, String path) {
        try {
            Stat stat = getZkClient(group).checkExists().forPath(path);
            return stat != null;
        } catch (Exception e) {
            logger.error("Path {} ,checkPathIsExist has exception e = {}:", path, e);
        }
        return false;
    }
    /**
     * 构造函数
     * 
     * @param connectStrMap  zookeeper客户端连接字符串Map, key:zk的group, value:连接字符串
     */
    public ConfigDelegateImpl(Map<String, String> connectStrMap) {
        this(DEFAULT_BASE_SLEEP_TIMEMS, DEFAULT_MAX_RETRIES, connectStrMap,
            DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS);
    }

    /**
     * get zookeeper client
     * @param group 分组
     * @return
     */
    private CuratorFramework getZkClient(String group) {
        CuratorFramework client = clientCache.get(group);
        if (client != null) {
            return client;
        } else {
            throw new IllegalArgumentException("unknow zookeeper group:" + group);
        }
    }

    /**
     *  会把path带上环境前缀
     * @param group 分组
     * @param path 路径
     * @param key key
     * @return
     */
    @Override
    public String get(String group, String path, String key) {
        try {
            String result = new String(getData(group, path), "UTF-8");
            if (!StringUtils.isEmpty(key)) {
                Map map = JSONObject.parseObject(result, Map.class);
                result = (String)map.get(key);
            }
            return result;
        } catch (Exception e) {
            logger.error("get config node error, path:" + path + " key:" + key, e);
            return null;
        }
    }

    /**
     * get data by group and path
     *
     * @param group group
     * @param path path
     */
    @Override
    public byte[] getData(String group, String path) {
        try {
            return getZkClient(group).getData().forPath(path);
        } catch (Exception e) {
            logger.error("get config node data error, path:" + path, e);
            return null;
        }
    }
    
    /**
     * 初始化zk连接
     * 
     * @param baseSleepTimeMs  initial amount of time to wait between retries
     * @param maxRetries  max number of times to retry
     * @param connectionString  zookeeper客户端连接字符串
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @return zk client
     */
    private CuratorFramework initClient(int baseSleepTimeMs, int maxRetries,
                                        String connectionString, int sessionTimeoutMs,
                                        int connectionTimeoutMs) {
        CuratorFramework client = getClient(baseSleepTimeMs,
            maxRetries, connectionString,
            sessionTimeoutMs, connectionTimeoutMs);
        
        return client;
    }

    /**
     * 获取客户端
     *
     * @param baseSleepTimeMs initial amount of time to wait between retries
     * @param maxRetries max number of times to retry
     * @param connectionString zookeeper客户端连接字符串
     * @param sessionTimeoutMs    session timeout
     * @param connectionTimeoutMs connection timeout
     * @return zookeeper 客户端
     */
    public static CuratorFramework getClient(int baseSleepTimeMs,
            int maxRetries, String connectionString,
            int sessionTimeoutMs, int connectionTimeoutMs) {
        RetryPolicy retryPolicy = new RetryNTimes(maxRetries, baseSleepTimeMs);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString,
                sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        client.start();

        return client;
    }

    /**
     * 配置数据
     * @param group 分组
     * @param path 路径
     * @return
     */
    @Override
    public boolean deletePath(String group, String path) {
        CuratorFramework cf = getZkClient(group);
        Stat stat = null;
        try {
            stat = cf.checkExists().forPath(path);
            if (stat != null) {
                cf.delete().forPath(path);
            }
        } catch (Exception e) {
            logger.error("Path {} ,deletePath has exception e = {}:", path, e);
            return false;
        }
        return true;
    }

    /**
     * 配置数据
     * @param group 分组
     * @param path 路径
     * @param data 数据
     * @return
     */
    @Override
    public String createOrderEphemeralPathAndSetData(String group, String path, String data) {
        CuratorFramework cf = getZkClient(group);
        Stat stat = null;
        String createPath = null;
        try {
            stat = cf.checkExists().forPath(path);
            if (stat == null) {
                createPath = cf.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                        .forPath(path, data.getBytes("UTF-8"));
            }
        } catch (Exception e) {
            logger.error("Path {} ,createOrderEphemeralPathAndSetData has exception e = {}:", path, e);
        }
        return createPath;
    }

    /**
     * 配置数据
     * @param group 分组
     * @param path 路径
     * @param data 数据
     * @return
     */
    @Override
    public boolean createEphemeralPathAndSetData(String group, String path, String data) {
        CuratorFramework cf = getZkClient(group);
        Stat stat = null;
        try {
            stat = cf.checkExists().forPath(path);
            if (stat == null) {
                cf.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL).forPath(path);
            }
            if (!StringUtils.isEmpty(data)) {
                cf.setData().forPath(path, data.getBytes("UTF-8"));
            }
        } catch (Exception e) {
            logger.error("Path {} ,createEphemeralPathAndSetData has exception e = {}:", path, e);
            return false;
        }
        return true;
    }

    @Override
    public boolean createEphemeralPathAndSetDataForClient(String path, String data) {
        Collection<CuratorFramework>  cfs = clientCache.values();
        if (cfs != null) {
            for (CuratorFramework cf : cfs) {
                Stat stat = null;
                try {
                    stat = cf.checkExists().forPath(path);
                    if (stat == null) {
                        cf.create().creatingParentsIfNeeded()
                                .withMode(CreateMode.EPHEMERAL).forPath(path);
                    }
                    if (!StringUtils.isEmpty(data)) {
                        cf.setData().forPath(path, data.getBytes("UTF-8"));
                    }
                } catch (Exception e) {
                    logger.error("Path {} ,"
                            + "createEphemeralPathAndSetDataForClient has exception e = {}:", path, e);
                    return false;
                }
            }
        }
        return true;
    }
    /**
     * 配置数据
     * @param group 分组
     * @param path 路径
     * @param data 数据
     * @return
     */
    @Override
    public void setOrderEphemeralPathData(String group, String path, String data) {
        CuratorFramework cf = getZkClient(group);
        try {
            if (!StringUtils.isEmpty(data)) {
                cf.setData().forPath(path, data.getBytes("UTF-8"));
            }
        } catch (Exception e) {
            logger.error("Path {} ,setOrderEphemeralPathData has exception e = {}:",
                    path, e);
        }
    }

    /**
     * 配置数据
     * @param group 分组
     * @param path 路径
     * @param data 数据
     * @return
     */
    @Override
    public void createPathAndSetData(String group, String path, String data) {
        CuratorFramework cf = getZkClient(group);
        Stat stat = null;
        try {
            stat = cf.checkExists().forPath(path);
            if (stat == null) {
                cf.create().creatingParentsIfNeeded().forPath(path, "".getBytes());
            }
            if (!StringUtils.isEmpty(data)) {
                cf.setData().forPath(path, data.getBytes("UTF-8"));
            }
        } catch (Exception e) {
            logger.error("Path {} ,createPathAndSetData has exception e = {}:",
                    path, e);
        }
    }

    /**
     * 创建路径
     * @param group 分组
     * @param path 路径
     * @return
     */
    @Override
    public boolean createIfNeededPath(String group, String path) {
        CuratorFramework cf = getZkClient(group);
        Stat stat = null;
        try {
            stat = cf.checkExists().forPath(path);
            if (stat == null) {
                cf.create().creatingParentsIfNeeded().forPath(path, "".getBytes());
            }
        } catch (Throwable e) {
            logger.error("Path {} ,createPath has exception e = {}:",
                    path, e);
            return false;
        }
        return true;
    }

    @Override
    public boolean addNodeListener(TreeCacheListener listener, String group, String path) {
        TreeCache treeCache = null;
        try {
            CuratorFramework client = getZkClient(group);
            treeCache = TreeCache.newBuilder(client, path)
                    .build();
            treeCache.getListenable().addListener(listener);
            treeCache.start();
            treeCacheMap.put(group+path, treeCache);
            return true;
        } catch (Exception e) {
            if (treeCache != null) {
                CloseableUtils.closeQuietly(treeCache);
            }
            logger.error("addNodeListener error", e);

            return false;
        }
    }

    @Override
    public boolean removeNodeListener(String group, String path) {
        try {
            TreeCache treeCache = treeCacheMap.remove(group+path);
            if (treeCache != null) {
                treeCache.close();
            }
            return true;
        } catch (Exception e) {
            logger.error("removeNodeListener error", e);
            return false;
        }
    }

    /**
     * 添加监听器
     *
     * @param listener 监听器
     * @return boolean
     */
    public boolean addChildNodeListener(PathChildrenCacheListener listener, String group, String path) {
        PathChildrenCache childrenCache = null;
        try {
            CuratorFramework client = getZkClient(group);
            childrenCache = new PathChildrenCache(client, path, true);
            childrenCache.getListenable().addListener(listener);
            childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            childrenCacheMap.put(group+path, childrenCache);
            return true;
        } catch (Exception e) {
            if (childrenCache != null) {
                CloseableUtils.closeQuietly(childrenCache);
            }
            logger.error("addNodeListener error", e);

            return false;
        }
    }

    /**
     * 删除监听器
     *
     * @param group group
     * @param path path
     * @return boolean
     */
    public boolean removeChildNodeListener(String group, String path) {
        try {
            PathChildrenCache childrenCache = childrenCacheMap.remove(group+path);
            if (childrenCache != null) {
                childrenCache.close();
            }
            return true;
        } catch (Exception e) {
            logger.error("removeNodeListener error", e);
            return false;
        }
    }

    /**
     * 获取路径下孩子节点
     * @param group 分组
     * @param path 路径
     * @return
     */
    @Override
    public List<String> getChildren(String group, String path) {
        CuratorFramework cf = getZkClient(group);
        List<String> list = null;
        try {
            list = cf.getChildren().forPath(path);
        } catch (Exception e) {
            logger.error("getChildren error e = {}", e);
        }
        return list;
    }

    /**
     * 获取路径下孩子节点的个数
     * @param group 分组
     * @param path 路径
     * @return
     */
    @Override
    public Integer getChildrenNum(String group, String path) {
        CuratorFramework cf = getZkClient(group);
        Integer num = -1;
        List<String> list = null;
        try {
            list = cf.getChildren().forPath(path);
            if (list != null) {
                num = list.size();
            } else {
                num = 0;
            }
        } catch (Exception e) {
            logger.error("Path {} ,getChildrenNum has exception e = {}:",
                    path, e);
        }
        return num;
    }

    /**
     * 获取当前路径的父路径
     * @param path 当前path
     * @return
     */
    private String getParentPath(String path) {
        String parentPath = "/";
        if (!StringUtils.isEmpty(path) && path.lastIndexOf("/") > 0) {
            parentPath = path.substring(0,path.lastIndexOf("/"));
        }
        return parentPath;
    }

    public void close() throws Exception {
        clientCache.entrySet().stream().forEach((e)->{
            e.getValue().close();
        });
    }
}
