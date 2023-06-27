/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export enum StatisticsTypeEnum {
  DataProxyIn = '5',
  DataProxyOut = '6',
}

export const StatisticsNameMap: Map<string, string> = (() => {
  return new Map([
    ['1', 'api接收成功'],
    ['2', 'api发送成功'],
    ['3', 'agent接收成功'],
    ['4', 'lagent发送成功'],
    [StatisticsTypeEnum.DataProxyIn, 'DataProxy接收成功'],
    [StatisticsTypeEnum.DataProxyOut, 'DataProxy发送成功'],
    ['7', 'sort-thive接收成功'],
    ['8', 'sort-thive发送成功'],
    ['9', 'sort-clickhouse接收成功'],
    ['10', 'sort-clickhouse发送成功'],
    ['11', 'sort-es接收成功'],
    ['12', 'sort-es发送成功'],
    ['13', 'starrocks接收成功'],
    ['14', 'starrocks发送成功'],
    ['15', 'sort-hudi接收成功'],
    ['16', 'sort-hudi发送成功'],
    ['17', 'sort-iceberg接收成功'],
    ['18', 'sort-iceberg发送成功'],
    ['19', 'sort-hbase接收成功'],
    ['20', 'sort-hbase发送成功'],
    ['21', 'sort-doris接收成功'],
    ['22', 'sort-doris发送成功'],
    ['23', 'sort-hive接收成功'],
    ['24', 'sort-hive发送成功'],
  ]);
})();

export const StatisticsColorMap: Map<string, string> = (() => {
  return new Map([
    ['1', '#006EFF'],
    ['2', '#29CC85'],
    ['3', '#FFBB00'],
    ['4', '#FF584C'],
    ['5', '#9741D9'],
    ['6', '#7DD936'],
    ['7', '#1FC0CC'],
    ['8', '#FF9C19'],
    ['9', '#E63984'],
    ['10', '#655CE6'],
    ['11', '#006EFF'],
    ['12', '#29CC85'],
    ['13', '#FFBB00'],
    ['14', '#FF584C'],
    ['15', '#9741D9'],
    ['16', '#7DD936'],
    ['17', '#1FC0CC'],
    ['18', '#FF9C19'],
    ['19', '#E63984'],
    ['20', '#655CE6'],
    ['21', '#006EFF'],
    ['22', '#29CC85'],
    ['23', '#FFBB00'],
    ['24', '#FF584C'],
  ]);
})();
