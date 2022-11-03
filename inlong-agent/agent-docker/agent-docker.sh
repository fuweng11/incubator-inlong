#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

file_path=$(cd "$(dirname "$0")"/../;pwd)
local_ip=$(ifconfig $ETH_NETWORK | grep "inet" | grep -v "inet6" | awk '{print $2}')
conf_file=${file_path}/conf/agent.properties
# config
sed -i "s/agent.fetcher.classname=.*$/agent.fetcher.classname=org.apache.inlong.agent.plugin.fetcher.ManagerFetcher/g" "${conf_file}"
sed -i "s/agent.local.ip=.*$/agent.local.ip=${local_ip}/g" "${conf_file}"
sed -i "s/agent.fetcher.interval=.*$/agent.fetcher.interval=${AGENT_FETCH_INTERVAL}/g" "${conf_file}"
sed -i "s/agent.heartbeat.interval=.*$/agent.heartbeat.interval=${AGENT_HEARTBEAT_INTERVAL}/g" "${conf_file}"
sed -i "s/agent.manager.vip.http.host=.*$/agent.manager.vip.http.host=${MANAGER_OPENAPI_IP}/g" "${conf_file}"
sed -i "s/agent.manager.vip.http.port=.*$/agent.manager.vip.http.port=${MANAGER_OPENAPI_PORT}/g" "${conf_file}"
sed -i "s/agent.http.port=.*$/agent.http.port=8008/g" "${conf_file}"
sed -i "s/agent.http.enable=.*$/agent.http.enable=true/g" "${conf_file}"
sed -i "s/agent.domainListeners=.*$/agent.domainListeners=org.apache.inlong.agent.metrics.AgentPrometheusMetricListener/g" "${conf_file}"
sed -i "s/agent.prometheus.exporter.port=.*$/agent.prometheus.exporter.port=9080/g" "${conf_file}"
sed -i "s/audit.proxys=.*$/audit.proxys=${AUDIT_PROXY_URL}/g" "${conf_file}"
sed -i "s/agent.cluster.tag=.*$/agent.cluster.tag=${CLUSTER_TAG}/g" "${conf_file}"
sed -i "s/agent.cluster.name=.*$/agent.cluster.name=${CLUSTER_NAME}/g" "${conf_file}"
sed -i "s/agent.cluster.inCharges=.*$/agent.cluster.inCharges=${CLUSTER_IN_CHARGES}/g" "${conf_file}"
sed -i "s/agent.manager.auth.secretId=.*$/agent.manager.auth.secretId=${MANAGER_OPENAPI_AUTH_ID}/g" "${conf_file}"
sed -i "s/agent.manager.auth.secretKey=.*$/agent.manager.auth.secretKey=${MANAGER_OPENAPI_AUTH_KEY}/g" "${conf_file}"
sed -i "s/agent.custom.fixed.ip=.*$/agent.custom.fixed.ip=${CUSTOM_FIXED_IP}/g" "${conf_file}"
sed -i "s/zhiyan.appMark=.*$/zhiyan.appMark=${ZHIYAN_APPMARK}/g" "${conf_file}"
sed -i "s/zhiyan.metricGroup=.*$/zhiyan.metricGroup=${ZHIYAN_METRICGROUP}/g" "${conf_file}"
sed -i "s/zhiyan.env=.*$/zhiyan.env=${ZHIYAN_ENV}/g" "${conf_file}"
sed -i "s/zhiyan.instanceMark=.*$/zhiyan.instanceMark=${local_ip}/g" "${conf_file}"

# start
bash +x ${file_path}/bin/agent.sh start
sleep 3
# keep alive
tail -F ${file_path}/logs/info.log
