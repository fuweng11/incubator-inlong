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

export enum DataLevelEnum {
  L1 = 0,
  L2 = 1,
  L3 = 2,
  L4 = 3,
  L5 = 4,
}

export const dataLevelMap: Map<DataLevelEnum, string> = (() => {
  const map = new Map();
  map
    .set(DataLevelEnum.L1, '一级')
    .set(DataLevelEnum.L2, '二级')
    .set(DataLevelEnum.L3, '三级')
    .set(DataLevelEnum.L4, '四级')
    .set(DataLevelEnum.L5, '五级');
  return map;
})();

export enum StatusEnum {
  WAIT_ONLINE = 0,
  ONLINE = 1,
  ERROR = 2,
  OUTLINE = 3,
}

export const statusMap: Map<
  StatusEnum,
  { label: string; colorTheme: 'success' | 'warning' | 'default' | 'danger' }
> = (() => {
  const map = new Map();
  map
    .set(StatusEnum.WAIT_ONLINE, {
      label: '待上线',
      colorTheme: 'default',
    })
    .set(StatusEnum.ONLINE, {
      label: '已上线',
      colorTheme: 'success',
    })
    .set(StatusEnum.ERROR, {
      label: '异常',
      colorTheme: 'warning',
    })
    .set(StatusEnum.OUTLINE, {
      label: '下线',
      colorTheme: 'error',
    });
  return map;
})();

export enum AccessTypeEnum {
  SDK = 1,
  AGENT = 2,
}

export const accessTypeMap: Map<AccessTypeEnum, string> = (() => {
  const map = new Map();
  map.set(AccessTypeEnum.SDK, 'SDK').set(AccessTypeEnum.AGENT, 'Agent');
  return map;
})();

export enum EncodeTypeEnum {
  UTF8 = 'UTF-8',
  GBK = 'GBK',
}

export const encodeTypeMap: Map<EncodeTypeEnum, string> = (() => {
  const map = new Map();
  map.set(EncodeTypeEnum.UTF8, 'UTF-8').set(EncodeTypeEnum.GBK, 'GBK');
  return map;
})();

export enum PeakRateEnum {
  L1 = 100,
  L2 = 1000,
  L3 = 5000,
  L4 = 10000,
  L5 = 50000,
  L6 = 100000,
  L7 = 500000,
  L8 = 1000000,
}

export const peakRateMap: Map<PeakRateEnum, string> = (() => {
  const map = new Map();
  map
    .set(PeakRateEnum.L1, '100及以下')
    .set(PeakRateEnum.L2, '100到1000之间')
    .set(PeakRateEnum.L3, '1000到5000之间')
    .set(PeakRateEnum.L4, '5000到1W之间')
    .set(PeakRateEnum.L5, '1W到5W之间')
    .set(PeakRateEnum.L6, '5W到10W之间')
    .set(PeakRateEnum.L7, '10W到50W之间')
    .set(PeakRateEnum.L8, '50W到100W之间');
  return map;
})();

export enum DataSeparatorEnum {
  Space = '32',
  VerticalLine = '124',
  Comma = '44',
  Semicolon = '59',
  Asterisk = '42',
  DoubleQuotes = '34',
}

export const dataSeparatorMap: Map<DataSeparatorEnum, string> = (() => {
  const map = new Map();
  map
    .set(DataSeparatorEnum.Space, '空格( )')
    .set(DataSeparatorEnum.VerticalLine, '竖线(|)')
    .set(DataSeparatorEnum.Comma, '逗号(,)')
    .set(DataSeparatorEnum.Semicolon, '分号(;)')
    .set(DataSeparatorEnum.Asterisk, '星号(*)')
    .set(DataSeparatorEnum.DoubleQuotes, '双引号(“)');
  return map;
})();
