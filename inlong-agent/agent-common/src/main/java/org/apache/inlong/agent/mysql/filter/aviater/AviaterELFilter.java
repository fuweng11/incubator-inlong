package org.apache.inlong.agent.mysql.filter.aviater;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.googlecode.aviator.AviatorEvaluator;
import org.apache.inlong.agent.mysql.filter.CanalEventFilter;
import org.apache.inlong.agent.mysql.filter.exception.CanalFilterException;
import org.apache.inlong.agent.plugin.dbsync.mysql.protocol.CanalEntry;

/**
 * 基于aviater el表达式的匹配过滤
 * 
 * @author jianghang 2012-7-23 上午10:46:32
 */
public class AviaterELFilter implements CanalEventFilter<CanalEntry.Entry> {

    public static final String ROOT_KEY = "entry";
    private String             expression;

    public AviaterELFilter(String expression){
        this.expression = expression;
    }

    public boolean filter(CanalEntry.Entry entry) throws CanalFilterException {
        if (StringUtils.isEmpty(expression)) {
            return true;
        }

        Map<String, Object> env = new HashMap<String, Object>();
        env.put(ROOT_KEY, entry);
        return (Boolean) AviatorEvaluator.execute(expression, env);
    }

}
