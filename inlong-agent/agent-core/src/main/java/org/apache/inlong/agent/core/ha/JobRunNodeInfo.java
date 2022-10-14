package org.apache.inlong.agent.core.ha;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

/**
 * 功能描述：job ha
 *
 * @Auther: nicobao
 * @Date: 2021/7/29 19:56
 * @Description:
 */
@Getter
@Setter
public class JobRunNodeInfo {

    private String ip;

    private long timeStamp;

    public String toString() {
        return JSONObject.toJSONString(this);
    }

}
