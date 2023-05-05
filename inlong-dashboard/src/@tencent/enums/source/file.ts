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

import type { FieldItemType } from './common';

export enum ReadModeEnum {
  FULL = 'FULL',
  INC = 'INCREMENT',
  DIY = 'DIY',
}

export const readModeMap: Map<ReadModeEnum, string> = (() => {
  return new Map([
    [ReadModeEnum.FULL, '全量'],
    [ReadModeEnum.INC, '增量'],
    [ReadModeEnum.DIY, '自定义开始时间'],
  ]);
})();

export const fields: FieldItemType[] = [
  { label: '集群名称', value: 'clusterName' },
  { label: '数据源IP', value: 'clusterIPs' },
  { label: '文件路径', value: 'filePath' },
  { label: '读取方式', value: 'readMode', enumMap: readModeMap },
];
