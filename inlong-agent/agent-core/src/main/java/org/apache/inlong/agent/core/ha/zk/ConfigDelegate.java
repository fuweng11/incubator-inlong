package org.apache.inlong.agent.core.ha.zk;


import java.util.List;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;


public interface ConfigDelegate extends AutoCloseable {

    String ZK_GROUP = "ZK_GROUP";

    String get(String group, String path, String key);

    byte[] getData(String group, String path);

    boolean checkPathIsExist(String group, String path);

    void createPathAndSetData(String group, String path, String data);

    String createOrderEphemeralPathAndSetData(String group, String path, String data);

    boolean deletePath(String group, String path);

    boolean createEphemeralPathAndSetData(String group, String path, String data);

    boolean createEphemeralPathAndSetDataForClient(String path, String data);

    void setOrderEphemeralPathData(String group, String path, String data);

    Integer getChildrenNum(String group, String path);

    List<String> getChildren(String group, String path);

    boolean createIfNeededPath(String group, String path);

    /**
     * 添加监听器
     *
     * @param listener 监听器
     * @return boolean
     */
    boolean addNodeListener(TreeCacheListener listener, String group, String path);

    /**
     * 删除监听器
     *
     * @param group group
     * @param path path
     * @return boolean
     */
    boolean removeNodeListener(String group, String path);

    /**
     * 添加监听器
     *
     * @param listener 监听器
     * @return boolean
     */
    boolean addChildNodeListener(PathChildrenCacheListener listener, String group, String path);

    /**
     * 删除监听器
     *
     * @param group group
     * @param path path
     * @return boolean
     */
    boolean removeChildNodeListener(String group, String path);

    /**
     * get syncId from zk path
     * @param zkPath zkPath
     * @return syncId
     */
    static String getClusterIdFromZKPath(String zkPath) {
        if (zkPath != null && zkPath.startsWith(Constants.SYNC_PREFIX)) {
            String[] p = zkPath.split(Constants.ZK_SEPARATOR);
            if(p != null && p.length >=3){
                return p[2];
            }
        }
        return null;
    }

    /**
     * get syncId from zk path
     * @param zkPath zkPath
     * @return syncId
     */
    static String getSyncIdFromZKPath(String zkPath) {
        if (zkPath != null && zkPath.startsWith(Constants.SYNC_PREFIX)) {
            String[] p = zkPath.split(Constants.ZK_SEPARATOR);
            if(p != null && p.length >=5){
                return p[4];
            }
        }
        return null;
    }

    /**
     * get syncId from zk path
     * @param zkPath zkPath
     * @return syncId
     */
    static String getLastFromZKPath(String zkPath) {
        if (zkPath != null && zkPath.startsWith(Constants.SYNC_JOB_PARENT_PATH)) {
            String[] p = zkPath.split(Constants.ZK_SEPARATOR);
            return p[p.length-1];
        }
        return null;
    }
}
