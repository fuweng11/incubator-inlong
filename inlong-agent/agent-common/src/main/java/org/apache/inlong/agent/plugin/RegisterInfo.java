package org.apache.inlong.agent.plugin;

import org.apache.inlong.agent.utils.JsonUtils.JSONArray;

@Deprecated
public interface RegisterInfo {
    /* 数据源id */
    String TASK_ID = "id";
    /* 操作类型: 1增、 2停 、3起、 4删、5改 */
    String STATUS = "status";
    /* 操作结果: 0成功/1失败 */
    String RESULT = "result";
    /* 操作结果信息: 成功/失败原因 */
    String MESSAGE = "message";
    /* 操作时间 */
    String OP_TIME = "modifyTime";

    String VERSION = "version";

    void addInfo(Integer taskId, Integer status, Integer version, int result, String message);

    JSONArray peekAllInfoJson();

    String peekAllInfo();

    void clear();
}
