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

package org.apache.inlong.dataproxy.channel;

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.utils.MessageUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.AbstractChannelSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FailoverChannelSelector extends AbstractChannelSelector {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverChannelSelector.class);

    private static final String SELECTOR_PROPS = "selector.";
    private static final String MASTER_CHANNEL = "master";
    private static final String FILE_METRIC_MASTER_CHANNEL = "fileMetricMaster";
    private static final String FILE_METRIC_SLAVE_CHANNEL = "fileMetricSlave";
    private static final String SLA_METRIC_CHANNEL = "slaMetric";
    private static final String ORDER_CHANNEL = "order";
    private static final String PULSAR_XFE_MASTER_CHANNEL = "pulsarXfeMaster";
    private static final String PULSAR_XFE_SLAVE_CHANNEL = "pulsarXfeSlave";
    // message channels
    private int msgMasterIndex = 0;
    private int msgSlaveIndex = 0;
    private final List<Channel> msgMasterChannels = new ArrayList<>();
    private final List<Channel> msgSlaveChannels = new ArrayList<>();
    // pulsar transfer channels
    private int pulsarXfeMasterIndex = 0;
    private int pulsarXfeSlaveIndex = 0;
    private final List<Channel> pulsarXfeMasterChannels = new ArrayList<>();
    private final List<Channel> pulsarXfeSlaveChannels = new ArrayList<>();
    private boolean ignorePulsarXfeError = true;
    // agent metric channels
    private int agentMetricMasterIndex = 0;
    private int agentMetricSlaveIndex = 0;
    private final List<Channel> agentMetricMasterChannels = new ArrayList<>();
    private final List<Channel> agentMetricSlaveChannels = new ArrayList<>();

    private final List<Channel> orderChannels = new ArrayList<>();
    private final List<Channel> slaMetricChannels = new ArrayList<>();

    @Override
    public List<Channel> getRequiredChannels(Event event) {
        List<Channel> retChannels = new ArrayList<>();
        if (event.getHeaders().containsKey(ConfigConstants.FILE_CHECK_DATA)) {
            retChannels.add(agentMetricMasterChannels.get(agentMetricMasterIndex));
            agentMetricMasterIndex = (agentMetricMasterIndex + 1) % agentMetricMasterChannels.size();
        } else if (event.getHeaders().containsKey(ConfigConstants.SLA_METRIC_DATA)) {
            retChannels.add(slaMetricChannels.get(0));
        } else if (MessageUtils.isSyncSendForOrder(event)) {
            String partitionKey = event.getHeaders().get(AttributeConstants.MESSAGE_PARTITION_KEY);
            if (partitionKey == null) {
                partitionKey = "";
            }
            int channelIndex = Math.abs(partitionKey.hashCode()) % orderChannels.size();
            retChannels.add(orderChannels.get(channelIndex));
        } else {
            retChannels.add(msgMasterChannels.get(msgMasterIndex));
            msgMasterIndex = (msgMasterIndex + 1) % msgMasterChannels.size();
        }
        return retChannels;
    }

    public List<Channel> getRequiredPulsarXfeChannels(Event event) {
        List<Channel> retChannels = new ArrayList<>();
        if (pulsarXfeMasterChannels.isEmpty()
                || event.getHeaders().containsKey(ConfigConstants.FILE_CHECK_DATA)) {
            return retChannels;
        }
        if (ConfigManager.getInstance().isRequirePulsarTransfer(
                event.getHeaders().get(AttributeConstants.GROUP_ID),
                event.getHeaders().get(AttributeConstants.STREAM_ID))) {
            retChannels.add(pulsarXfeMasterChannels.get(pulsarXfeMasterIndex));
            pulsarXfeMasterIndex = (pulsarXfeMasterIndex + 1) % pulsarXfeMasterChannels.size();
        }
        return retChannels;
    }

    @Override
    public List<Channel> getOptionalChannels(Event event) {
        List<Channel> retChannels = new ArrayList<>();
        if (event.getHeaders().containsKey(ConfigConstants.FILE_CHECK_DATA)) {
            retChannels.add(agentMetricSlaveChannels.get(agentMetricSlaveIndex));
            agentMetricSlaveIndex = (agentMetricSlaveIndex + 1) % agentMetricSlaveChannels.size();
        } else if (event.getHeaders().containsKey(ConfigConstants.SLA_METRIC_DATA)) {
            retChannels.add(slaMetricChannels.get(1));
        } else {
            retChannels.add(msgSlaveChannels.get(msgSlaveIndex));
            msgSlaveIndex = (msgSlaveIndex + 1) % msgSlaveChannels.size();
        }
        return retChannels;
    }

    public List<Channel> getOptionalPulsarXfeChannels(Event event) {
        List<Channel> retChannels = new ArrayList<>();
        if (pulsarXfeSlaveChannels.isEmpty()
                || event.getHeaders().containsKey(ConfigConstants.FILE_CHECK_DATA)) {
            return retChannels;
        }
        if (ConfigManager.getInstance().isRequirePulsarTransfer(
                event.getHeaders().get(AttributeConstants.GROUP_ID),
                event.getHeaders().get(AttributeConstants.STREAM_ID))) {
            retChannels.add(pulsarXfeSlaveChannels.get(pulsarXfeSlaveIndex));
            pulsarXfeSlaveIndex = (pulsarXfeSlaveIndex + 1) % pulsarXfeSlaveChannels.size();
        }
        return retChannels;
    }

    public boolean isIgnoreXfeError() {
        return ignorePulsarXfeError;
    }

    /**
     * split channel name into name list.
     *
     * @param channelName - channel name
     * @return - name list
     */
    private List<String> splitChannelName(String channelName) {
        List<String> fileMetricList = new ArrayList<String>();
        if (StringUtils.isNotBlank(channelName)) {
            fileMetricList = Arrays.asList(channelName.split("\\s+"));
        }
        return fileMetricList;
    }

    @Override
    public void configure(Context context) {
        // LOG.info(context.toString());
        String masters = context.getString(MASTER_CHANNEL);
        String fileMerticMaster = context.getString(FILE_METRIC_MASTER_CHANNEL);
        String fileMerticSlave = context.getString(FILE_METRIC_SLAVE_CHANNEL);
        String pulsarXfeMaster = context.getString(PULSAR_XFE_MASTER_CHANNEL);
        String pulsarXfeSlave = context.getString(PULSAR_XFE_SLAVE_CHANNEL);
        String slaMetric = context.getString(SLA_METRIC_CHANNEL);
        String orderMetric = context.getString(ORDER_CHANNEL);
        if (StringUtils.isEmpty(masters)) {
            throw new FlumeException("master channel is null!");
        }
        List<String> masterList = splitChannelName(masters);
        List<String> fileMetricMasterList = splitChannelName(fileMerticMaster);
        List<String> fileMetricSlaveList = splitChannelName(fileMerticSlave);
        List<String> pulsarXfeMasterList = splitChannelName(pulsarXfeMaster);
        List<String> pulsarXfeSlaveList = splitChannelName(pulsarXfeSlave);
        List<String> slaMetricList = splitChannelName(slaMetric);
        List<String> orderMetricList = splitChannelName(orderMetric);
        // classify channels
        for (Map.Entry<String, Channel> entry : getChannelNameMap().entrySet()) {
            String channelName = entry.getKey();
            Channel channel = entry.getValue();
            if (masterList.contains(channelName)) {
                this.msgMasterChannels.add(channel);
            } else if (fileMetricMasterList.contains(channelName)) {
                this.agentMetricMasterChannels.add(channel);
            } else if (fileMetricSlaveList.contains(channelName)) {
                this.agentMetricSlaveChannels.add(channel);
            } else if (pulsarXfeMasterList.contains(channelName)) {
                this.pulsarXfeMasterChannels.add(channel);
            } else if (pulsarXfeSlaveList.contains(channelName)) {
                this.pulsarXfeSlaveChannels.add(channel);
            } else if (slaMetricList.contains(channelName)) {
                this.slaMetricChannels.add(channel);
            } else if (orderMetricList.contains(channelName)) {
                this.orderChannels.add(channel);
            } else {
                this.msgSlaveChannels.add(channel);
            }
        }
        LOG.info(
                "Configure channels, msgMasters={}, msgSlaves={}, agentMetricMaster={}, agentMetricSlave={}, pulsarXfeMaster={}, pulsarXfeSlave={}, orders={}, slaMetrics={}",
                this.msgMasterChannels, this.msgSlaveChannels,
                this.agentMetricMasterChannels, this.agentMetricSlaveChannels,
                this.pulsarXfeMasterChannels, this.pulsarXfeSlaveChannels,
                this.orderChannels, this.slaMetricChannels);
    }
}
