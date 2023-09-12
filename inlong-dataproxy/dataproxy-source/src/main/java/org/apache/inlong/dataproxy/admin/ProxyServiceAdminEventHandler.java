/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.admin;

import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.holder.TDBankMetaConfigHolder;
import org.apache.inlong.dataproxy.config.pojo.RmvDataItem;
import org.apache.inlong.sdk.commons.admin.AbstractAdminEventHandler;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.servlet.http.HttpServletResponse;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import static org.apache.inlong.dataproxy.admin.ProxyServiceMBean.MBEAN_TYPE;

/**
 * StopServiceAdminEventHandler
 */
public class ProxyServiceAdminEventHandler extends AbstractAdminEventHandler {

    /**
     * configure
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
    }

    /**
     * process
     *
     * @param cmd
     * @param event
     * @param response
     */
    @Override
    public void process(String cmd, Event event, HttpServletResponse response) {
        String sourceName = event.getHeaders().get(ProxyServiceMBean.KEY_SOURCENAME);
        LOG.info("start to process admin task:{},sourceName:{}", cmd, sourceName);
        switch (cmd) {
            case ProxyServiceMBean.METHOD_STOPSERVICE:
            case ProxyServiceMBean.METHOD_RECOVERSERVICE: {
                if (sourceName == null) {
                    break;
                }
                if (StringUtils.equals(sourceName, ProxyServiceMBean.ALL_SOURCENAME)) {
                    this.processAll(cmd, event, response);
                } else {
                    this.processOne(cmd, sourceName, response);
                }
                break;
            }

            case TDBankMetaConfigHolder.REMOVE_META_ITEMS: {
                processRmvMetaData(event, response);
                break;
            }

            default:
                break;
        }
        LOG.info("end to process admin task:{},sourceName:{}", cmd, sourceName);
    }

    private void processRmvMetaData(Event event, HttpServletResponse response) {
        String rmvConfigJsonStr = event.getHeaders().get(TDBankMetaConfigHolder.REMOVE_ITEMS_KEY);
        LOG.info("Received manual remove meta-data request, remove targets are {}", rmvConfigJsonStr);
        if (StringUtils.isBlank(rmvConfigJsonStr)) {
            this.outputResponse(response,
                    "Manual remove meta-data failure: the data to be deleted is blank!");
            LOG.warn("Received manual remove meta-data request: the data to be deleted is blank!");
            return;
        }
        Gson gson = new Gson();
        Type type = new TypeToken<List<RmvDataItem>>() {
        }.getType();
        List<RmvDataItem> origRmvItems = gson.fromJson(rmvConfigJsonStr, type);
        if (origRmvItems == null || origRmvItems.isEmpty()) {
            this.outputResponse(response,
                    "Manual remove meta-data failure: the data to be deleted translate to json failure!");
            LOG.warn("Received manual remove meta-data request: the data to be deleted to object empty!");
            return;
        }
        if (ConfigManager.getInstance().manualRmvMetaConfig(origRmvItems)) {
            this.outputResponse(response, "Manual remove meta-data success!");
            LOG.info("Received manual remove meta-data request: operation successful");
        } else {
            this.outputResponse(response,
                    "Manual remove meta-data failure: please check data and contact the administrator!");
            LOG.info("Received manual remove meta-data request: operation failure");
        }

    }

    /**
     * processOne
     *
     * @param cmd
     * @param sourceName
     * @param response
     */
    private void processOne(String cmd, String sourceName, HttpServletResponse response) {
        LOG.info("start to processOne admin task:{},sort task:{}", cmd, sourceName);
        StringBuilder result = new StringBuilder();
        try {
            String beanName = JMX_DOMAIN + DOMAIN_SEPARATOR
                    + JMX_TYPE + PROPERTY_EQUAL + MBEAN_TYPE + PROPERTY_SEPARATOR
                    + JMX_NAME + PROPERTY_EQUAL + sourceName;

            ObjectName objName = new ObjectName(beanName);
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectInstance mbean = mbs.getObjectInstance(objName);
            LOG.info("getObjectInstance for type:{},name:{},result:{}", MBEAN_TYPE, sourceName, mbean);
            String className = mbean.getClassName();
            Class<?> clazz = ClassUtils.getClass(className);
            if (ClassUtils.isAssignable(clazz, ProxyServiceMBean.class)) {
                mbs.invoke(mbean.getObjectName(), cmd, null, null);
                result.append(String.format("Execute command:%s success in bean:%s\n",
                        cmd, mbean.getObjectName().toString()));
            }
            this.outputResponse(response, result.toString());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            this.outputResponse(response, result.toString());
        }
        LOG.info("end to processOne admin task:{},sort task:{}", cmd, sourceName);
    }

    /**
     * processAll
     *
     * @param cmd
     * @param event
     * @param response
     */
    private void processAll(String cmd, Event event, HttpServletResponse response) {
        LOG.info("start to processAll admin task:{}", cmd);
        StringBuilder result = new StringBuilder();
        try {
            String beanName = JMX_DOMAIN + DOMAIN_SEPARATOR
                    + JMX_TYPE + PROPERTY_EQUAL + MBEAN_TYPE + PROPERTY_SEPARATOR
                    + "*";
            ObjectName objName = new ObjectName(beanName.toString());
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            Set<ObjectInstance> mbeans = mbs.queryMBeans(objName, null);
            LOG.info("queryMBeans for type:{},result:{}", MBEAN_TYPE, mbeans);
            for (ObjectInstance mbean : mbeans) {
                ObjectName beanObjectName = mbean.getObjectName();
                String className = mbean.getClassName();
                Class<?> clazz = ClassUtils.getClass(className);
                if (ClassUtils.isAssignable(clazz, ProxyServiceMBean.class)) {
                    mbs.invoke(mbean.getObjectName(), cmd, null, null);
                    result.append(String.format("Execute command:%s success in bean:%s\n",
                            cmd, beanObjectName.toString()));
                }
            }
            this.outputResponse(response, result.toString());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            this.outputResponse(response, result.toString());
        }
        LOG.info("end to processAll admin task:{}", cmd);
    }
}
